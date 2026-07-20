package app

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	sourceRepositoryInstallationHeader = "X-Deployer-Repository-Installation-Id"
	sourceRepositoryIDHeader           = "X-Deployer-Repository-Id"
	sourceRepositoryNameHeader         = "X-Deployer-Repository-Full-Name"
	sourceCommitSHAHeader              = "X-Deployer-Commit-Sha"
	sourceArtifactDigestHeader         = "X-Deployer-Artifact-Digest"
)

var prepareHostingSource = prepareHostingBrokerSource

var errHostingSourceArtifactMismatch = errors.New("source artifact digest mismatch")
var errHostingSourceBrokerUnavailable = errors.New("source broker unavailable")

var sourceArtifactPublishLocks [64]sync.Mutex

type hostingSourceRedeemRequest struct {
	Reference string `json:"reference"`
}

type hostingSourceBrokerClient struct {
	baseURL string
	token   string
	client  *http.Client
	maxSize int64
}

func newHostingSourceBrokerClient(cfg AppConfig) (*hostingSourceBrokerClient, error) {
	if strings.TrimSpace(cfg.HostingSourceBrokerURL) == "" || strings.TrimSpace(cfg.HostingSourceBrokerToken) == "" {
		return nil, fmt.Errorf("hosting source broker is not configured")
	}
	parsed, err := url.Parse(cfg.HostingSourceBrokerURL)
	if err != nil || !validPrivateServiceURL(parsed) {
		return nil, fmt.Errorf("DEPLOYER_HOSTING_SOURCE_BROKER_URL must use HTTPS (or loopback HTTP) without credentials, query, or fragment")
	}
	timeout := cfg.HostingSourceBrokerTimeout
	if timeout <= 0 {
		timeout = 4 * time.Minute
	}
	if cfg.ServerWriteTimeout > 0 && timeout >= cfg.ServerWriteTimeout {
		return nil, fmt.Errorf("DEPLOYER_HOSTING_SOURCE_BROKER_TIMEOUT must be shorter than DEPLOYER_SERVER_WRITE_TIMEOUT")
	}
	maxSize := cfg.HostingSourceMaxBytes
	if maxSize <= 0 {
		return nil, fmt.Errorf("DEPLOYER_HOSTING_SOURCE_MAX_BYTES must be positive")
	}
	return &hostingSourceBrokerClient{
		baseURL: strings.TrimRight(cfg.HostingSourceBrokerURL, "/"),
		token:   cfg.HostingSourceBrokerToken,
		maxSize: maxSize,
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("source broker redirects are not allowed")
			},
		},
	}, nil
}

func prepareHostingBrokerSource(ctx context.Context, project *HostingProject, source HostingSourceReference, commitSHA, expectedDigest string) (string, string, error) {
	client, err := newHostingSourceBrokerClient(appConfig)
	if err != nil {
		return "", "", err
	}
	return client.fetch(ctx, project, source, commitSHA, expectedDigest)
}

func (client *hostingSourceBrokerClient) fetch(ctx context.Context, project *HostingProject, source HostingSourceReference, commitSHA, expectedDigest string) (string, string, error) {
	if project == nil || project.RepositoryInstallationID <= 0 || project.RepositoryID <= 0 ||
		!repositoryNamePattern.MatchString(project.RepositoryFullName) {
		return "", "", fmt.Errorf("provisioned repository identity is invalid")
	}
	if source.Provider != "control-plane" || !safeSourceReference(source.Reference) ||
		len(source.Reference) < 8 || len(source.Reference) > 256 || !source.ExpiresAt.After(time.Now()) {
		return "", "", fmt.Errorf("source reference is invalid or expired")
	}
	if len(commitSHA) != 40 || !isLowerHex(commitSHA) {
		return "", "", fmt.Errorf("commit identity is invalid")
	}
	if !validSHA256Digest(expectedDigest) {
		return "", "", fmt.Errorf("expected source artifact digest is invalid")
	}

	encoded, err := json.Marshal(hostingSourceRedeemRequest{Reference: source.Reference})
	if err != nil {
		return "", "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		client.baseURL+"/api/internal/v1/source-artifacts/redeem", bytes.NewReader(encoded))
	if err != nil {
		return "", "", err
	}
	installationID := strconv.FormatInt(project.RepositoryInstallationID, 10)
	repositoryID := strconv.FormatInt(project.RepositoryID, 10)
	request.Header.Set("Authorization", "Bearer "+client.token)
	request.Header.Set("Accept", "application/x-tar")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(sourceRepositoryInstallationHeader, installationID)
	request.Header.Set(sourceRepositoryIDHeader, repositoryID)
	request.Header.Set(sourceRepositoryNameHeader, project.RepositoryFullName)
	request.Header.Set(sourceCommitSHAHeader, commitSHA)
	request.Header.Set(sourceArtifactDigestHeader, expectedDigest)

	response, err := client.client.Do(request)
	if err != nil {
		return "", "", fmt.Errorf("%w: fetch source artifact: %v", errHostingSourceBrokerUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
			return "", "", fmt.Errorf("%w: source broker returned status %d", errHostingSourceBrokerUnavailable, response.StatusCode)
		}
		return "", "", fmt.Errorf("source broker returned status %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/x-tar" {
		return "", "", fmt.Errorf("source broker returned an invalid content type")
	}
	if response.ContentLength <= 0 || response.ContentLength > client.maxSize {
		return "", "", fmt.Errorf("source artifact size is missing or exceeds the configured limit")
	}
	if response.Header.Get(sourceRepositoryInstallationHeader) != installationID ||
		response.Header.Get(sourceRepositoryIDHeader) != repositoryID ||
		response.Header.Get(sourceRepositoryNameHeader) != project.RepositoryFullName ||
		response.Header.Get(sourceCommitSHAHeader) != commitSHA {
		return "", "", fmt.Errorf("source broker repository or commit identity does not match the deployment")
	}
	brokerDigest := strings.ToLower(strings.TrimSpace(response.Header.Get(sourceArtifactDigestHeader)))
	if !validSHA256Digest(brokerDigest) {
		return "", "", fmt.Errorf("source broker returned an invalid artifact digest")
	}
	if brokerDigest != expectedDigest {
		return "", brokerDigest, errHostingSourceArtifactMismatch
	}
	storage := currentArtifactStorage()
	if err := storage.Ensure(); err != nil {
		return "", "", err
	}
	finalPath := managedArtifactPath("hosting-source-" + strings.TrimPrefix(expectedDigest, "sha256:") + ".tar")
	temporaryPath, temporary, err := prepareManagedArtifactUpload(finalPath)
	if err != nil {
		return "", "", err
	}
	committed := false
	defer func() {
		if !committed {
			abortManagedArtifactUpload(temporaryPath)
		}
	}()
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(response.Body, client.maxSize+1))
	closeErr := temporary.Close()
	if copyErr != nil {
		return "", "", fmt.Errorf("read source artifact: %w", copyErr)
	}
	if closeErr != nil {
		return "", "", closeErr
	}
	if written != response.ContentLength || written > client.maxSize {
		return "", "", fmt.Errorf("source artifact length does not match the broker response")
	}
	digest := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if digest != brokerDigest {
		return "", digest, errHostingSourceArtifactMismatch
	}
	archive, err := storage.Open(temporaryPath)
	if err != nil {
		return "", "", err
	}
	archiveErr := validateHostingSourceArchive(archive, client.maxSize)
	closeArchiveErr := archive.Close()
	if archiveErr != nil {
		return "", "", archiveErr
	}
	if closeArchiveErr != nil {
		return "", "", closeArchiveErr
	}

	lockDigest := sha256.Sum256([]byte(digest))
	lockIndex := int(lockDigest[0]) % len(sourceArtifactPublishLocks)
	sourceArtifactPublishLocks[lockIndex].Lock()
	defer sourceArtifactPublishLocks[lockIndex].Unlock()
	if info, err := storage.Stat(finalPath); err == nil {
		if !info.Mode().IsRegular() {
			return "", "", fmt.Errorf("managed source artifact is not a regular file")
		}
		existingDigest, digestErr := managedArtifactSHA256(finalPath)
		if digestErr != nil || existingDigest != digest {
			return "", "", fmt.Errorf("managed source artifact identity conflict")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	if err := commitManagedArtifactUpload(temporaryPath, finalPath); err != nil {
		return "", "", err
	}
	committed = true
	return finalPath, digest, nil
}

func managedArtifactSHA256(artifactPath string) (string, error) {
	file, err := currentArtifactStorage().Open(artifactPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func validateHostingSourceArchive(archive io.Reader, maximumBytes int64) error {
	reader := tar.NewReader(archive)
	seen := make(map[string]struct{})
	var entries int
	var expandedBytes int64
	for {
		header, err := reader.Next()
		if err == io.EOF {
			if entries == 0 {
				return fmt.Errorf("source archive contains no entries")
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("source archive is not a valid tar: %w", err)
		}
		entries++
		if entries > 100000 {
			return fmt.Errorf("source archive contains too many entries")
		}
		clean := path.Clean(header.Name)
		if clean == "." || path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("source archive path is unsafe")
		}
		if _, duplicate := seen[clean]; duplicate {
			return fmt.Errorf("source archive contains a duplicate path")
		}
		seen[clean] = struct{}{}
		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 || header.Size > maximumBytes-expandedBytes {
				return fmt.Errorf("source archive expanded size exceeds the configured limit")
			}
			expandedBytes += header.Size
		default:
			return fmt.Errorf("source archive contains a forbidden non-regular entry")
		}
	}
}
