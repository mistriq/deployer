package controlplane

import (
	"testing"

	"github.com/mistriq/deployer/internal/runtimeengine"
)

func TestEdgeDomains(t *testing.T) {
	route := runtimeengine.Route{Hostname: "site.apps.example.com", Aliases: []string{"www.client.cz"}, HTTPS: true, CertStatus: "valid"}
	got := edgeDomains(route, []string{"www.client.cz", "shop.client.cz"})
	if len(got) != 3 || !got[0].Primary || !got[0].HTTPS {
		t.Fatalf("primary host missing: %+v", got)
	}
	if !got[1].HTTPS {
		t.Fatalf("served alias must report HTTPS: %+v", got[1])
	}
	if got[2].HTTPS {
		t.Fatalf("alias not yet served must not report HTTPS: %+v", got[2])
	}
	route.CertStatus = "pending"
	if edgeDomains(route, []string{"www.client.cz"})[1].HTTPS {
		t.Fatal("pending certificate must not report HTTPS")
	}
}
