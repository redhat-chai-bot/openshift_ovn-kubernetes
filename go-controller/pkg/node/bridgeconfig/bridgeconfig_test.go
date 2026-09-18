// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package bridgeconfig

import (
	"fmt"
	"testing"

	"github.com/k8snetworkplumbingwg/sriovnet"
	"github.com/onsi/gomega"

	"k8s.io/utils/ptr"

	libovsdbclient "github.com/ovn-kubernetes/libovsdb/client"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	uplinkv1alpha1 "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/uplink/v1alpha1"
	ovsops "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/libovsdb/ops"
	nodetypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/node/types"
	ovntest "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/testing"
	libovsdbtest "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/testing/libovsdb"
	ovntypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	utilmocks "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util/mocks"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/vswitchd"
)

func TestGetStaticFDBPort(t *testing.T) {
	tests := []struct {
		name     string
		bridge   *BridgeConfiguration
		expected string
	}{
		{
			name: "uses bridge when representor is absent",
			bridge: &BridgeConfiguration{
				bridgeName: "br-ex",
			},
			expected: "br-ex",
		},
		{
			name: "uses representor when present",
			bridge: &BridgeConfiguration{
				bridgeName: "ovsbr1",
				gwIfaceRep: "pf0hpf",
			},
			expected: "pf0hpf",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.bridge.GetStaticFDBPort(); got != tc.expected {
				t.Fatalf("expected static FDB port %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestRefreshExternalIngressPortRoles(t *testing.T) {
	newClient := func(t *testing.T, physicalOfport, defaultOfport, sourceOfport int, includeProvider bool) libovsdbclient.Client {
		t.Helper()
		bridgeUUID := "bridge-uuid"
		physPortUUID := "phys-port-uuid"
		defaultPortUUID := "default-port-uuid"
		sourcePortUUID := "source-port-uuid"
		ports := []string{physPortUUID, defaultPortUUID, sourcePortUUID}
		data := []libovsdbtest.TestData{
			&vswitchd.OpenvSwitch{UUID: "root-ovs", Bridges: []string{bridgeUUID}},
			&vswitchd.Port{UUID: physPortUUID, Name: "uplink-port", Interfaces: []string{"phys-iface"}},
			&vswitchd.Interface{UUID: "phys-iface", Name: "eth0", Type: "system", Ofport: ptr.To(physicalOfport)},
			&vswitchd.Port{UUID: defaultPortUUID, Name: "default-gr-port", Interfaces: []string{"default-gr-iface"}},
			&vswitchd.Interface{UUID: "default-gr-iface", Name: "patch-default-to-br-int", Type: "patch", Ofport: ptr.To(defaultOfport)},
			&vswitchd.Port{UUID: sourcePortUUID, Name: "blue-gr-port", Interfaces: []string{"blue-gr-iface"}},
			&vswitchd.Interface{UUID: "blue-gr-iface", Name: "patch-blue-to-br-int", Type: "patch", Ofport: ptr.To(sourceOfport)},
		}
		if includeProvider {
			ports = append(ports, "provider-port-uuid")
			data = append(data,
				&vswitchd.Port{UUID: "provider-port-uuid", Name: "provider-port", ExternalIDs: map[string]string{"ovn-localnet-port": ""}, Interfaces: []string{"provider-iface"}},
				&vswitchd.Interface{UUID: "provider-iface", Name: "patch-blue_ovn_localnet_port-to-br-int", Type: "patch", Ofport: ptr.To(8)},
			)
		}
		data = append(data, &vswitchd.Bridge{UUID: bridgeUUID, Name: "br-ex", Ports: ports})
		client, cleanup, err := libovsdbtest.NewOVSTestHarness(libovsdbtest.TestSetup{OVSData: data})
		if err != nil {
			t.Fatalf("failed to create OVSDB test harness: %v", err)
		}
		t.Cleanup(cleanup.Cleanup)
		return client
	}

	bridge := &BridgeConfiguration{
		ovsClient:  newClient(t, 7, 9, 10, true),
		bridgeName: "br-ex",
		uplinkName: "eth0",
		netConfig: map[string]*BridgeUDNConfiguration{
			ovntypes.DefaultNetworkName: {PatchPort: "patch-default-to-br-int", MasqCTMark: nodetypes.CtMarkOVN},
			"blue":                      {PatchPort: "patch-blue-to-br-int", MasqCTMark: "0x4"},
		},
	}

	changed, err := bridge.RefreshExternalIngressPortRoles("blue")
	if err != nil || !changed {
		t.Fatalf("initial role refresh = (%t, %v), want (true, nil)", changed, err)
	}
	if got, want := bridge.ExternalIngressPortRoles(), (ExternalIngressPortRoles{
		PhysicalUplink:        "7",
		ProviderLocalnetPorts: []string{"8"},
		GRControlPorts:        []string{"10", "9"},
		DesignatedSourcePatch: "10",
	}); !got.equal(want) {
		t.Fatalf("initial roles = %#v, want %#v", got, want)
	}
	changed, err = bridge.RefreshExternalIngressPortRoles("blue")
	if err != nil || changed {
		t.Fatalf("unchanged role refresh = (%t, %v), want (false, nil)", changed, err)
	}

	// A fresh OVSDB cache models port recreation/removal: all remaining
	// ofports changed and the provider localnet port disappeared.
	bridge.ovsClient = newClient(t, 17, 19, 20, false)
	changed, err = bridge.RefreshExternalIngressPortRoles("blue")
	if err != nil || !changed {
		t.Fatalf("changed role refresh = (%t, %v), want (true, nil)", changed, err)
	}
	if got, want := bridge.ExternalIngressPortRoles(), (ExternalIngressPortRoles{
		PhysicalUplink:        "17",
		GRControlPorts:        []string{"19", "20"},
		DesignatedSourcePatch: "20",
	}); !got.equal(want) {
		t.Fatalf("refreshed roles = %#v, want %#v", got, want)
	}
	if got := bridge.netConfig[ovntypes.DefaultNetworkName].OfPortPatch; got != "19" {
		t.Fatalf("default GR ofport = %q, want 19", got)
	}
	if got := bridge.netConfig["blue"].OfPortPatch; got != "20" {
		t.Fatalf("blue GR ofport = %q, want 20", got)
	}
}

func TestClassifyExternalIngressPortRolesKeepsSharedPortsOutOfProviderRole(t *testing.T) {
	bridge := &BridgeConfiguration{
		netConfig: map[string]*BridgeUDNConfiguration{
			ovntypes.DefaultNetworkName: {PatchPort: "patch-default-to-br-int", MasqCTMark: nodetypes.CtMarkOVN},
		},
	}
	roles := bridge.classifyExternalIngressPortRoles([]ovsops.OVSBridgePort{{
		Port: &vswitchd.Port{ExternalIDs: map[string]string{"ovn-localnet-port": ""}},
		Interfaces: []*vswitchd.Interface{{
			Name: "patch-default-to-br-int", Type: "patch", Ofport: ptr.To(9),
		}},
	}}, ovntypes.DefaultNetworkName)
	if len(roles.ProviderLocalnetPorts) != 0 || len(roles.SharedProviderPorts) != 1 || roles.SharedProviderPorts[0] != "9" {
		t.Fatalf("shared provider/GR port must remain unsupported, got %#v", roles)
	}
}

func TestGatewayHostOVSInterfaceResolvesSmartNICRepresentor(t *testing.T) {
	g := gomega.NewWithT(t)
	fexec := ovntest.NewFakeExec()
	fexec.AddFakeCmd(&ovntest.ExpectedCmd{
		Cmd:    "ovs-vsctl --timeout=15 port-to-br pf0vf1",
		Stderr: "no bridge for pf0vf1",
		Err:    fmt.Errorf("not an OVS port"),
	})
	fexec.AddFakeCmd(&ovntest.ExpectedCmd{
		Cmd:    "ovs-vsctl --timeout=15 port-to-br pf0vf1_rep",
		Output: "ovsbr1",
	})
	g.Expect(util.SetExec(fexec)).To(gomega.Succeed())
	t.Cleanup(util.ResetRunner)

	fsOps := utilmocks.NewFileSystemOps(t)
	origFSOps := util.GetFileSystemOps()
	util.SetFileSystemOps(fsOps)
	t.Cleanup(func() {
		util.SetFileSystemOps(origFSOps)
	})
	fsOps.On("Readlink", "/sys/class/net/pf0vf1/device").
		Return("../../0000:00:00.1", nil)

	sriovOps := utilmocks.NewSriovnetOps(t)
	origSriovOps := util.GetSriovnetOps()
	util.SetSriovnetOpsInst(sriovOps)
	t.Cleanup(func() {
		util.SetSriovnetOpsInst(origSriovOps)
	})
	sriovOps.On("GetUplinkRepresentor", "0000:00:00.1").Return("pf0", nil)
	sriovOps.On("GetVfIndexByPciAddress", "0000:00:00.1").Return(1, nil)
	sriovOps.On("GetVfRepresentor", "pf0", 1).Return("pf0vf1_rep", nil)

	rep, err := gatewayHostOVSInterface("ovsbr1", "pf0vf1")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(rep).To(gomega.Equal("pf0vf1_rep"))
	g.Expect(fexec.CalledMatchesExpected()).To(gomega.BeTrue(), fexec.ErrorDesc())
}

// newDPUUnmanagedBridgeHarness prepares the scaffolding shared by the
// unmanaged bridge configuration tests: a DPU-mode node config restored on
// cleanup, an OVSDB with one bridge holding the eth1 uplink and one
// (Port, Interface) pair per representor name, a fake exec that answers the
// eth1 ofport probe, and a fresh sriovnet mock installed for the test.
func newDPUUnmanagedBridgeHarness(t *testing.T, bridgeName string, bridgeExternalIDs map[string]string,
	repNames ...string) (libovsdbclient.Client, *ovntest.FakeExec, *utilmocks.SriovnetOps) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(config.PrepareTestConfig()).To(gomega.Succeed())
	t.Cleanup(func() {
		_ = config.PrepareTestConfig()
		util.ResetRunner()
	})
	config.IPv4Mode = false
	config.OvnKubeNode.Mode = ovntypes.NodeModeDPU

	bridgeUUID := bridgeName + "-uuid"
	portUUIDs := []string{"eth1-port-uuid"}
	ovsData := []libovsdbtest.TestData{
		&vswitchd.Port{UUID: "eth1-port-uuid", Name: "eth1", Interfaces: []string{"eth1-interface-uuid"}},
		&vswitchd.Interface{UUID: "eth1-interface-uuid", Name: "eth1", Type: "system"},
	}
	for _, repName := range repNames {
		portUUID := repName + "-port-uuid"
		interfaceUUID := repName + "-interface-uuid"
		portUUIDs = append(portUUIDs, portUUID)
		ovsData = append(ovsData,
			&vswitchd.Port{UUID: portUUID, Name: repName, Interfaces: []string{interfaceUUID}},
			&vswitchd.Interface{UUID: interfaceUUID, Name: repName, Type: "system"},
		)
	}
	ovsData = append(ovsData,
		&vswitchd.OpenvSwitch{UUID: "root-ovs", Bridges: []string{bridgeUUID}},
		&vswitchd.Bridge{
			UUID:        bridgeUUID,
			Name:        bridgeName,
			Ports:       portUUIDs,
			ExternalIDs: bridgeExternalIDs,
		},
	)
	ovsClient, ovsCleanup, err := libovsdbtest.NewOVSTestHarness(libovsdbtest.TestSetup{OVSData: ovsData})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	t.Cleanup(ovsCleanup.Cleanup)

	fexec := ovntest.NewLooseCompareFakeExec()
	fexec.AddFakeCmd(&ovntest.ExpectedCmd{
		Cmd:    "ovs-vsctl --timeout=15 get interface eth1 ofport",
		Output: "7",
	})
	g.Expect(util.SetExec(fexec)).To(gomega.Succeed())

	sriovOps := utilmocks.NewSriovnetOps(t)
	origSriovOps := util.GetSriovnetOps()
	util.SetSriovnetOpsInst(sriovOps)
	t.Cleanup(func() {
		util.SetSriovnetOpsInst(origSriovOps)
	})
	return ovsClient, fexec, sriovOps
}

func TestNewUnmanagedBridgeConfigurationResolvesDPUHostRepresentor(t *testing.T) {
	g := gomega.NewWithT(t)
	ovsClient, fexec, sriovOps := newDPUUnmanagedBridgeHarness(t, "ovsbr1",
		map[string]string{"bridge-uplink": "eth1"}, "pfhpf0")
	// FindHostRepresentorByPeerMAC walks the bridge ports in map order and
	// returns on the first match, so expectations for the other ports may
	// legitimately go unused.
	sriovOps.On("GetRepresentorPortFlavour", "eth1").
		Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_UNKNOWN), fmt.Errorf("not a representor")).
		Maybe()
	sriovOps.On("GetRepresentorPortFlavour", "pfhpf0").
		Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_PF), nil)
	sriovOps.On("GetDevlinkPortFunctionMacAddress", "pfhpf0").
		Return(ovntest.MustParseMAC("00:11:22:33:44:55"), nil)

	bridge, err := NewUnmanagedBridgeConfiguration(
		ovsClient,
		"ovsbr1",
		"pf0",
		"node-a",
		"physnet-blue",
		ovntest.MustParseIPNets("172.28.0.2/24"),
		ovntest.MustParseMAC("00:11:22:33:44:55"),
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "the PF-backed unmanaged bridge configuration must succeed")
	g.Expect(bridge.GetGatewayIfaceRep()).To(gomega.Equal("pfhpf0"),
		"the PF representor must be selected as the gateway representor")
	g.Expect(bridge.GetStaticFDBPort()).To(gomega.Equal("pfhpf0"),
		"the static FDB entry must be pinned to the PF representor")
	g.Expect(fexec.CalledMatchesExpected()).To(gomega.BeTrue(), fexec.ErrorDesc())
}

// TestNewUnmanagedBridgeConfigurationResolvesDPUHostFunctionRepresentor covers
// the common host VF layout: the host assigns its own VF MAC, so the DPU
// eswitch reports no function MAC for the VF representor and only the published
// host function indices can identify it.
func TestNewUnmanagedBridgeConfigurationResolvesDPUHostFunctionRepresentor(t *testing.T) {
	g := gomega.NewWithT(t)
	ovsClient, fexec, sriovOps := newDPUUnmanagedBridgeHarness(t, "br-hostvf0", nil, "pf0vf0")
	sriovOps.On("GetVfRepresentorDPU", "0", "0").Return("pf0vf0", nil)
	sriovOps.On("GetRepresentorPortFlavour", "eth1").
		Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_UNKNOWN), fmt.Errorf("not a representor"))
	sriovOps.On("GetRepresentorPortFlavour", "pf0vf0").
		Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_VF), nil)
	// The eswitch holds no function MAC for a host assigned VF MAC, so the
	// representor offers no identity to verify against.
	sriovOps.On("GetDevlinkPortFunctionMacAddress", "pf0vf0").
		Return(nil, fmt.Errorf("devlink port function for netdev pf0vf0 does not report a usable hardware address"))

	bridge, err := NewUnmanagedBridgeConfiguration(
		ovsClient,
		"br-hostvf0",
		"enp4s0f0v0",
		"node-a",
		"physnet-blue",
		ovntest.MustParseIPNets("172.28.0.2/24"),
		ovntest.MustParseMAC("4e:68:cd:ae:2b:f5"),
		&uplinkv1alpha1.HostFunction{PFID: 0, VFID: ptr.To(int32(0))},
	)
	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"an unreadable eswitch function MAC must not block gateway representor resolution")
	g.Expect(bridge.GetGatewayIfaceRep()).To(gomega.Equal("pf0vf0"),
		"the representor of the published host function must be selected as the gateway representor")
	g.Expect(bridge.GetStaticFDBPort()).To(gomega.Equal("pf0vf0"),
		"the static FDB entry must be pinned to the host function representor")
	g.Expect(fexec.CalledMatchesExpected()).To(gomega.BeTrue(), fexec.ErrorDesc())
}

// TestDPUGatewayRepresentorFallback covers the non-happy branches of
// dpuGatewayRepresentor: the MAC scan fallback when the host function lookup
// fails or resolves a representor that is not on the bridge, and the combined
// error when both resolution paths fail.
func TestDPUGatewayRepresentorFallback(t *testing.T) {
	hostMAC := ovntest.MustParseMAC("4e:68:cd:ae:2b:f5")
	hostFunction := &uplinkv1alpha1.HostFunction{PFID: 0, VFID: ptr.To(int32(0))}

	// The MAC scan walks the bridge ports in map order, so the eth1 probe may
	// legitimately go unused when pf0vf0 matches first.
	mockUplinkNotARepresentor := func(sriovOps *utilmocks.SriovnetOps) {
		sriovOps.On("GetRepresentorPortFlavour", "eth1").
			Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_UNKNOWN), fmt.Errorf("not a representor")).
			Maybe()
	}

	t.Run("falls back to the MAC scan when the host function lookup fails", func(t *testing.T) {
		g := gomega.NewWithT(t)
		ovsClient, _, sriovOps := newDPUUnmanagedBridgeHarness(t, "br-hostvf0", nil, "pf0vf0")
		sriovOps.On("GetVfRepresentorDPU", "0", "0").
			Return("", fmt.Errorf("failed to get VF representor"))
		mockUplinkNotARepresentor(sriovOps)
		sriovOps.On("GetRepresentorPortFlavour", "pf0vf0").
			Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_VF), nil)
		sriovOps.On("GetDevlinkPortFunctionMacAddress", "pf0vf0").
			Return(hostMAC, nil)

		bridge, err := ovsops.GetBridge(ovsClient, "br-hostvf0")
		g.Expect(err).NotTo(gomega.HaveOccurred())
		rep, err := dpuGatewayRepresentor(ovsClient, bridge, hostFunction, hostMAC, "node-a")
		g.Expect(err).NotTo(gomega.HaveOccurred(),
			"a failed host function lookup must fall back to the MAC scan")
		g.Expect(rep).To(gomega.Equal("pf0vf0"))
	})

	t.Run("falls back to the MAC scan when the resolved representor is not on the bridge", func(t *testing.T) {
		g := gomega.NewWithT(t)
		ovsClient, _, sriovOps := newDPUUnmanagedBridgeHarness(t, "br-hostvf0", nil, "pf0vf0")
		// The indices resolve a representor that is not attached to the
		// uplink bridge, with no eswitch function MAC to veto it early.
		sriovOps.On("GetVfRepresentorDPU", "0", "0").Return("pf9vf9", nil)
		sriovOps.On("GetRepresentorPortFlavour", "pf9vf9").
			Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_VF), nil)
		sriovOps.On("GetDevlinkPortFunctionMacAddress", "pf9vf9").
			Return(nil, fmt.Errorf("no usable hardware address"))
		mockUplinkNotARepresentor(sriovOps)
		sriovOps.On("GetRepresentorPortFlavour", "pf0vf0").
			Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_VF), nil)
		sriovOps.On("GetDevlinkPortFunctionMacAddress", "pf0vf0").
			Return(hostMAC, nil)

		bridge, err := ovsops.GetBridge(ovsClient, "br-hostvf0")
		g.Expect(err).NotTo(gomega.HaveOccurred())
		rep, err := dpuGatewayRepresentor(ovsClient, bridge, hostFunction, hostMAC, "node-a")
		g.Expect(err).NotTo(gomega.HaveOccurred(),
			"a representor on another bridge must be rejected and fall back to the MAC scan")
		g.Expect(rep).To(gomega.Equal("pf0vf0"))
	})

	t.Run("combines the errors when both resolution paths fail", func(t *testing.T) {
		g := gomega.NewWithT(t)
		ovsClient, _, sriovOps := newDPUUnmanagedBridgeHarness(t, "br-hostvf0", nil, "pf0vf0")
		sriovOps.On("GetVfRepresentorDPU", "0", "0").
			Return("", fmt.Errorf("failed to get VF representor"))
		mockUplinkNotARepresentor(sriovOps)
		// The only representor on the bridge peers with some other host MAC.
		sriovOps.On("GetRepresentorPortFlavour", "pf0vf0").
			Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_VF), nil)
		sriovOps.On("GetDevlinkPortFunctionMacAddress", "pf0vf0").
			Return(ovntest.MustParseMAC("00:11:22:33:44:66"), nil)

		bridge, err := ovsops.GetBridge(ovsClient, "br-hostvf0")
		g.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = dpuGatewayRepresentor(ovsClient, bridge, hostFunction, hostMAC, "node-a")
		g.Expect(err).To(gomega.MatchError(util.ErrHostRepresentorNotFound),
			"the MAC scan miss must stay inspectable in the combined error")
		g.Expect(err.Error()).To(gomega.ContainSubstring("host function pf0vf0 did not resolve one either"),
			"the combined error must carry the host function failure as well")
	})
}

func TestNewUnmanagedBridgeConfigurationResolvesDPUHostVFRepresentor(t *testing.T) {
	g := gomega.NewWithT(t)
	// No bridge-uplink external-id: the physical uplink must be derived by
	// skipping the VF representors.
	ovsClient, fexec, sriovOps := newDPUUnmanagedBridgeHarness(t, "br-hostvf0", nil, "pf0vf0", "pf0vf7")
	// The walk returns on the first match (pf0vf7), so the probes of the
	// other ports may legitimately go unused depending on map order.
	sriovOps.On("GetRepresentorPortFlavour", "eth1").
		Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_UNKNOWN), fmt.Errorf("not a representor")).
		Maybe()
	sriovOps.On("GetRepresentorPortFlavour", "pf0vf0").
		Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_VF), nil).
		Maybe()
	sriovOps.On("GetDevlinkPortFunctionMacAddress", "pf0vf0").
		Return(ovntest.MustParseMAC("00:11:22:33:44:66"), nil).
		Maybe()
	sriovOps.On("GetRepresentorPortFlavour", "pf0vf7").
		Return(sriovnet.PortFlavour(sriovnet.PORT_FLAVOUR_PCI_VF), nil)
	sriovOps.On("GetDevlinkPortFunctionMacAddress", "pf0vf7").
		Return(ovntest.MustParseMAC("00:11:22:33:44:55"), nil)

	bridge, err := NewUnmanagedBridgeConfiguration(
		ovsClient,
		"br-hostvf0",
		"enp4s0f0v0",
		"node-a",
		"physnet-blue",
		ovntest.MustParseIPNets("172.28.0.2/24"),
		ovntest.MustParseMAC("00:11:22:33:44:55"),
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "the VF-backed unmanaged bridge configuration must succeed")
	g.Expect(bridge.GetGatewayIfaceRep()).To(gomega.Equal("pf0vf7"),
		"the VF representor whose peer MAC matches must be selected as the gateway representor")
	g.Expect(bridge.GetStaticFDBPort()).To(gomega.Equal("pf0vf7"),
		"the static FDB entry must be pinned to the matching VF representor")
	g.Expect(fexec.CalledMatchesExpected()).To(gomega.BeTrue(), fexec.ErrorDesc())
}
