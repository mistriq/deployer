package controlplane

import "errors"

var errTargetUnknown = errors.New("RUNTIME_TARGET_UNKNOWN")
var errTargetLocked = errors.New("RUNTIME_TARGET_LOCKED")

// Historical records always belong to the legacy target, even when the default
// for newly created projects changes. Target IDs must never be reassigned to nodes.
func stableTarget(id string) string {
	if id == "" {
		return "default"
	}
	return id
}
func (s *Service) runtimeFor(id string) (Runtime, error) {
	id = stableTarget(id)
	if rt := s.Runtimes[id]; rt != nil {
		return rt, nil
	}
	if id == "default" && s.Runtime != nil {
		return s.Runtime, nil
	}
	return nil, errTargetUnknown
}
func (s *Service) knownTarget(id string) bool {
	if !identifier.MatchString(id) {
		return false
	}
	if s.Runtimes == nil && id == "default" {
		return true
	}
	_, err := s.runtimeFor(id)
	return err == nil
}
