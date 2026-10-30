// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package macbinding

import (
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	factorymocks "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory/mocks"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	libovsdbtest "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/testing/libovsdb"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

// recorder is a sink for the reconcile keys a controller.Reconciler receives, so
// tests can assert which follow-up work the controller enqueued.
type recorder struct {
	mu   sync.Mutex
	keys []string
}

func (r *recorder) record(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
}

func (r *recorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.keys))
	copy(out, r.keys)
	return out
}

// startRecorder builds a real controller.Reconciler whose only job is to record
// the keys it is handed (controller.Reconciler has unexported methods, so it
// cannot be faked outside the package). The caller must controller.Stop it.
func startRecorder(g gomega.Gomega, name string) (*recorder, controller.Reconciler) {
	rec := &recorder{}
	r := controller.NewReconciler(name, &controller.ReconcilerConfig{
		RateLimiter: controller.DefaultRateLimiter[string](),
		Reconcile:   func(key string) error { rec.record(key); return nil },
		Threadiness: 1,
		MaxAttempts: 1,
	})
	g.Expect(controller.Start(r)).To(gomega.Succeed())
	return rec, r
}

// cdnPortFor returns the CDN Gateway Router external port name for a node, the
// same way the controller derives it.
func cdnPortFor(node string) string {
	return types.GWRouterToExtSwitchPrefix + (&util.DefaultNetInfo{}).GetNetworkScopedGWRouterName(node)
}

// fakeUplinkSourceProvider provides the designated uplink sources without a real
// openflow manager. An empty map leaves only the default group (the getter adds
// the CDN source for the "" uplink itself).
type fakeUplinkSourceProvider struct {
	sources map[string]string
}

func (f *fakeUplinkSourceProvider) GetMacBindingSourceForUplinks() map[string]string {
	return f.sources
}

func (f *fakeUplinkSourceProvider) RegisterUplinkCallback(func(string)) {}

// newTestController builds a controller wired only with the fields the
// method-level tests exercise, bypassing NewMACBindingController (which needs a
// live networkManager and watchFactory).
func newTestController() *MACBindingController {
	return &MACBindingController{
		uplinkSourceProvider: &fakeUplinkSourceProvider{sources: map[string]string{}},
		nodeName:             "node1",
		followers:            map[string]sets.Set[string]{},
		nodeIPs:              map[string]map[string]string{},
	}
}

// TestReconcileUplinkSource verifies that a re-designation notification for a
// network that is not already a source invalidates the cached uplink->source
// designation — making a full reconcile due (hasUnknownSource) — and enqueues
// the network so the group is recomputed.
func TestReconcileUplinkSource(t *testing.T) {
	g := gomega.NewWithT(t)
	g.Expect(config.PrepareTestConfig()).To(gomega.Succeed())

	const network = "tenantred"

	netRec, netR := startRecorder(g, "network")
	defer controller.Stop(netR)

	c := newTestController()
	c.cdnGatewayPort = cdnPortFor("node1")
	c.networkReconciler = netR
	c.uplinkSourceProvider = &fakeUplinkSourceProvider{sources: map[string]string{"uplinkB": "rtoe-GR_other_node1"}}
	// prime a valid designation so hasUnknownSource is only true if it gets invalidated.
	c.getMacBindingSourceForUplinks()
	g.Expect(c.hasUnknownSource()).To(gomega.BeFalse())

	c.ReconcileUplinkSource(network)

	// the designation was invalidated, so a full reconcile is now due...
	g.Expect(c.hasUnknownSource()).To(gomega.BeTrue())
	// ...and the network is enqueued for recompute.
	g.Eventually(netRec.got).Should(gomega.ConsistOf(network))
}

type lifecycleNetworkManager struct {
	networkmanager.Interface
	nadRemoved bool
	refRemoved bool
}

func (manager *lifecycleNetworkManager) RegisterNADReconciler(networkmanager.NADReconciler) uint64 {
	return 1
}

func (manager *lifecycleNetworkManager) RegisterNetworkRefReconciler(networkmanager.NetworkRefReconciler) uint64 {
	return 2
}

func (manager *lifecycleNetworkManager) DeRegisterNADReconciler(registration uint64) {
	manager.nadRemoved = registration == 1
}

func (manager *lifecycleNetworkManager) DeRegisterNetworkRefReconciler(registration uint64) {
	manager.refRemoved = registration == 2
}

type lifecycleUplinkProvider struct {
	registrations chan bool
}

func (provider *lifecycleUplinkProvider) GetMacBindingSourceForUplinks() map[string]string {
	return map[string]string{}
}

func (provider *lifecycleUplinkProvider) RegisterUplinkCallback(callback func(string)) {
	provider.registrations <- callback != nil
}

func TestMACBindingRunStopsWithoutClosingSharedClients(t *testing.T) {
	assert := gomega.NewWithT(t)
	assert.Expect(config.PrepareTestConfig()).To(gomega.Succeed())
	nbClient, nbCleanup, err := libovsdbtest.NewNBTestHarness(libovsdbtest.TestSetup{}, nil)
	assert.Expect(err).NotTo(gomega.HaveOccurred())
	defer nbCleanup.Cleanup()
	sbClient, sbCleanup, err := libovsdbtest.NewSBTestHarness(libovsdbtest.TestSetup{}, nil)
	assert.Expect(err).NotTo(gomega.HaveOccurred())
	defer sbCleanup.Cleanup()
	nodeInformer := informers.NewSharedInformerFactory(fake.NewClientset(), 0).Core().V1().Nodes()
	watchFactory := &factorymocks.NodeWatchFactory{}
	watchFactory.On("NodeCoreInformer").Return(nodeInformer)
	watchFactory.On("GetNodes").Return(nil, nil)
	watchFactory.On("GetNode", "node1").Return(nodeIPTestNode("node1", nodeIPMAC1, []string{"10.0.0.1/24"}), nil)
	manager := &lifecycleNetworkManager{Interface: networkmanager.Default().Interface()}
	provider := &lifecycleUplinkProvider{registrations: make(chan bool, 2)}
	bindingController := NewMACBindingController(nbClient, sbClient, watchFactory, manager, provider, "node1")
	stop := make(chan struct{})
	finished := make(chan error, 1)
	go func() { finished <- bindingController.Run(stop) }()
	select {
	case registered := <-provider.registrations:
		assert.Expect(registered).To(gomega.BeTrue())
	case <-time.After(5 * time.Second):
		close(stop)
		t.Fatal("controller did not register uplink callback")
	}
	close(stop)
	select {
	case err := <-finished:
		assert.Expect(err).NotTo(gomega.HaveOccurred())
	case <-time.After(5 * time.Second):
		t.Fatal("controller failed to stop")
	}
	assert.Expect(<-provider.registrations).To(gomega.BeFalse())
	assert.Expect(manager.nadRemoved).To(gomega.BeTrue())
	assert.Expect(manager.refRemoved).To(gomega.BeTrue())
	assert.Expect(nbClient.Connected()).To(gomega.BeTrue())
	assert.Expect(sbClient.Connected()).To(gomega.BeTrue())
}
