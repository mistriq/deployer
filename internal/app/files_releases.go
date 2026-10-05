package app

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type filesReleaseLayout struct{ root, releases, shared, metadata string }

func newFilesReleaseLayout(deployDir string) (filesReleaseLayout, error) {
	abs, err := filepath.Abs(deployDir)
	if err != nil {
		return filesReleaseLayout{}, err
	}
	if filepath.Clean(abs) == string(filepath.Separator) {
		return filesReleaseLayout{}, fmt.Errorf("deploy directory must not be root")
	}
	root := filepath.Join(filepath.Dir(abs), ".deployer-releases-"+filepath.Base(abs))
	return filesReleaseLayout{root, filepath.Join(root, "releases"), filepath.Join(root, "shared"), filepath.Join(root, "metadata")}, nil
}

// prepareFilesRelease validates and retains a release without changing the
// active release. The caller activates it after its other gates are ready.
func prepareFilesRelease(deployDir string, releaseID int64, archivePath string, preservePaths []string) error {
	if releaseID <= 0 {
		return fmt.Errorf("release ID must be positive")
	}
	layout, err := newFilesReleaseLayout(deployDir)
	if err != nil {
		return err
	}
	preservePaths, err = normalizedSharedPaths(preservePaths)
	if err != nil {
		return err
	}
	for _, dir := range []string{layout.releases, layout.shared, layout.metadata} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	digest, err := fileSHA256(archivePath)
	if err != nil {
		return err
	}
	id := strconv.FormatInt(releaseID, 10)
	final := filepath.Join(layout.releases, id)
	digestPath := filepath.Join(layout.metadata, id+".sha256")
	if _, err := os.Lstat(final); err == nil {
		stored, readErr := os.ReadFile(digestPath)
		if readErr == nil && strings.TrimSpace(string(stored)) == digest {
			return nil
		}
		return fmt.Errorf("release %d already exists with different or unverifiable artifact", releaseID)
	} else if !os.IsNotExist(err) {
		return err
	}
	temp, err := os.MkdirTemp(layout.releases, ".staging-")
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(temp)
		}
	}()
	// Validate fully before persistent shared state can be seeded or changed.
	if err := extractTarGz(archivePath, temp); err != nil {
		return fmt.Errorf("stage release %d: %w", releaseID, err)
	}
	if err := os.Rename(temp, final); err != nil {
		return fmt.Errorf("retain release %d: %w", releaseID, err)
	}
	keep = true
	if err := os.WriteFile(digestPath, []byte(digest+"\n"), 0644); err != nil {
		return err
	}
	return nil
}

func activateFilesRelease(deployDir string, releaseID int64, preservePaths []string) error {
	layout, err := newFilesReleaseLayout(deployDir)
	if err != nil {
		return err
	}
	target := filepath.Join(layout.releases, strconv.FormatInt(releaseID, 10))
	info, err := os.Stat(target)
	if err != nil {
		return fmt.Errorf("release %d unavailable: %w", releaseID, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("release %d is not a directory", releaseID)
	}
	preservePaths, err = normalizedSharedPaths(preservePaths)
	if err != nil {
		return err
	}
	absDeploy, _ := filepath.Abs(deployDir)
	rel, err := filepath.Rel(filepath.Dir(absDeploy), target)
	if err != nil {
		return err
	}
	tempLink := filepath.Join(filepath.Dir(absDeploy), ".deployer-activate-"+filepath.Base(absDeploy)+"-"+strconv.FormatInt(releaseID, 10))
	_ = os.Remove(tempLink)
	if err := os.Symlink(rel, tempLink); err != nil {
		return err
	}
	defer os.Remove(tempLink)
	current, err := os.Lstat(absDeploy)
	if os.IsNotExist(err) {
		if err := migrateSharedPaths("", target, layout, preservePaths); err != nil {
			return err
		}
		return os.Rename(tempLink, absDeploy)
	}
	if err != nil {
		return err
	}
	if current.Mode()&os.ModeSymlink != 0 {
		resolved, resolveErr := filepath.EvalSymlinks(absDeploy)
		if resolveErr != nil {
			return resolveErr
		}
		relManaged, relErr := filepath.Rel(layout.releases, resolved)
		if relErr != nil || relManaged == ".." || strings.HasPrefix(relManaged, ".."+string(filepath.Separator)) {
			return fmt.Errorf("deployment symlink is not managed by Deployer")
		}
		if err := migrateSharedPaths(resolved, target, layout, preservePaths); err != nil {
			return err
		}
		return os.Rename(tempLink, absDeploy)
	}
	backup := filepath.Join(layout.root, "pre-release-deployment")
	if _, err := os.Lstat(backup); err == nil {
		return fmt.Errorf("pre-release backup already exists at %s", backup)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(absDeploy, backup); err != nil {
		return fmt.Errorf("preserve existing deployment: %w", err)
	}
	if err := migrateSharedPaths(backup, target, layout, preservePaths); err != nil {
		_ = os.Rename(backup, absDeploy)
		return err
	}
	if err := os.Rename(tempLink, absDeploy); err != nil {
		_ = os.Rename(backup, absDeploy)
		return fmt.Errorf("activate release %d: %w", releaseID, err)
	}
	return nil
}

func normalizedSharedPaths(paths []string) ([]string, error) {
	cleaned := make([]string, 0, len(paths))
	for _, path := range paths {
		clean, err := cleanRelativeDeployPath(path)
		if err != nil {
			return nil, fmt.Errorf("invalid preserve path %q: %w", path, err)
		}
		cleaned = append(cleaned, clean)
	}
	sort.Strings(cleaned)
	for i, path := range cleaned {
		if i > 0 && (path == cleaned[i-1] || strings.HasPrefix(path, cleaned[i-1]+string(filepath.Separator))) {
			return nil, fmt.Errorf("overlapping preserve paths %q and %q", cleaned[i-1], path)
		}
	}
	return cleaned, nil
}

func migrateSharedPaths(currentDir, targetDir string, layout filesReleaseLayout, paths []string) error {
	for _, path := range paths {
		shared, dst := filepath.Join(layout.shared, path), filepath.Join(targetDir, path)
		if _, err := os.Lstat(shared); os.IsNotExist(err) {
			source := ""
			if currentDir != "" {
				candidate := filepath.Join(currentDir, path)
				if _, sourceErr := os.Lstat(candidate); sourceErr == nil {
					source = candidate
				} else if !os.IsNotExist(sourceErr) {
					return sourceErr
				}
			}
			if source == "" {
				if _, sourceErr := os.Lstat(dst); sourceErr == nil {
					source = dst
				} else if !os.IsNotExist(sourceErr) {
					return sourceErr
				}
			}
			if source != "" {
				if err := os.MkdirAll(filepath.Dir(shared), 0755); err != nil {
					return err
				}
				if err := os.Rename(source, shared); err != nil {
					return err
				}
			} else {
				continue
			}
		} else if err != nil {
			return err
		}
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		rel, err := filepath.Rel(filepath.Dir(dst), shared)
		if err != nil {
			return err
		}
		if err := os.Symlink(rel, dst); err != nil {
			return err
		}
		if currentDir != "" && filepath.Clean(currentDir) != filepath.Clean(targetDir) {
			currentPath := filepath.Join(currentDir, path)
			if info, statErr := os.Lstat(currentPath); statErr == nil {
				if info.Mode()&os.ModeSymlink != 0 {
					resolved, resolveErr := filepath.EvalSymlinks(currentPath)
					if resolveErr == nil && filepath.Clean(resolved) == filepath.Clean(shared) {
						continue
					}
				}
				return fmt.Errorf("mutable path %s exists alongside shared state; refusing to discard it", path)
			} else if !os.IsNotExist(statErr) {
				return statErr
			}
			if err := os.MkdirAll(filepath.Dir(currentPath), 0755); err != nil {
				return err
			}
			currentRel, err := filepath.Rel(filepath.Dir(currentPath), shared)
			if err != nil {
				return err
			}
			if err := os.Symlink(currentRel, currentPath); err != nil {
				return err
			}
		}
	}
	return nil
}
