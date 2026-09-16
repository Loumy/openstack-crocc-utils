// openstack-cleaner nettoie l'ensemble des ressources d'un projet OpenStack.
//
// Usage:
//   export OS_CLOUD=mycloud
//   go run main.go --project mon-projet                 # dry-run (défaut)
//   go run main.go --project mon-projet --dry-run=false # suppression réelle
//
package main

import (
    "errors"
    "flag"
    "fmt"
    "log/slog"
    "os"
    "sort"
    "strings"
    "sync"
    "time"

    "github.com/gophercloud/gophercloud"
    "github.com/gophercloud/gophercloud/openstack"
    "github.com/gophercloud/gophercloud/openstack/blockstorage/v3/snapshots"
    "github.com/gophercloud/gophercloud/openstack/blockstorage/v3/volumes"
    "github.com/gophercloud/gophercloud/openstack/compute/v2/servers"
    "github.com/gophercloud/gophercloud/openstack/identity/v3/projects"
    "github.com/gophercloud/gophercloud/openstack/imageservice/v2/images"
    "github.com/gophercloud/gophercloud/openstack/networking/v2/extensions/layer3/routers"
    "github.com/gophercloud/gophercloud/openstack/networking/v2/ports"
    "github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/shares"
    "github.com/gophercloud/gophercloud/openstack/networking/v2/extensions/layer3/floatingips"
    "github.com/gophercloud/gophercloud/openstack/networking/v2/networks"
    "github.com/gophercloud/gophercloud/openstack/networking/v2/subnets"
    "github.com/gophercloud/utils/openstack/clientconfig"
)

/* -------------------------------------------------------------------------- */
/*                          Constantes & présentation                          */
/* -------------------------------------------------------------------------- */

const (
    colReset  = "\033[0m"
    colRed    = "\033[31m"
    colGreen  = "\033[32m"
    colYellow = "\033[33m"
    colBlue   = "\033[34m"
    colCyan   = "\033[36m"
    colBold   = "\033[1m"
    colGrey   = "\033[90m"
)

var useColor = true

func c(color, s string) string {
    if !useColor {
        return s
    }
    return color + s + colReset
}

var (
    symOK    = func() string { return c(colGreen, "✓") }
    symFail  = func() string { return c(colRed, "✗") }
    symInfo  = func() string { return c(colBlue, "ℹ") }
    symDry   = func() string { return c(colYellow, "◌") }
    symWarn  = func() string { return c(colYellow, "⚠") }
    symArrow = func() string { return c(colCyan, "→") }
)

// ResourceKind identifie un type de ressource géré par l'outil.
type ResourceKind string
 
const (
    KindServer     ResourceKind = "compute/instance"
    KindSnapshot   ResourceKind = "blockstorage/snapshot"
    KindVolume     ResourceKind = "blockstorage/volume"
    KindShare      ResourceKind = "sharedfs/share"
    KindImage      ResourceKind = "image"
    KindFloatingIP ResourceKind = "network/floatingip"
    KindPort       ResourceKind = "network/port"
    KindRouter     ResourceKind = "network/router"
    KindNetwork    ResourceKind = "network/network"
)


// ordre de suppression imposé (respect des dépendances)
var deletionOrder = []ResourceKind{
    KindServer,     // a. instances compute (libèrent volumes attachés + ports)
    KindSnapshot,   // b1. snapshots avant volumes (dépendance)
    KindVolume,     // b2. volumes
    KindShare,      // c. partages Manila
    KindImage,      // d. images
    KindFloatingIP, // e. floating IPs (avant les ports : une FIP associée bloque le port)
    KindPort,       // f. ports / interfaces réseau
    KindRouter,     // g. routeurs
    KindNetwork,    // h. réseaux (et leurs subnets, en dernier)
}

/* -------------------------------------------------------------------------- */
/*                                  Modèles                                    */
/* -------------------------------------------------------------------------- */

// Resource est la représentation normalisée d'une ressource OpenStack.
type Resource struct {
    Kind   ResourceKind
    ID     string
    Name   string
    Status string
    Extra  string // informations complémentaires (taille, IP, device owner…)
}

func (r Resource) label() string {
    name := r.Name
    if name == "" {
        name = "<sans nom>"
    }
    out := fmt.Sprintf("%s %s", name, c(colGrey, "("+r.ID+")"))
    if r.Status != "" {
        out += " " + c(colGrey, "["+r.Status+"]")
    }
    if r.Extra != "" {
        out += " " + c(colGrey, r.Extra)
    }
    return out
}

// OpError enregistre une erreur contextualisée pour le rapport final.
type OpError struct {
    Phase    string // "list" | "delete" | "wait"
    Kind     ResourceKind
    Resource string
    Err      error
}

func (e OpError) String() string {
    res := e.Resource
    if res == "" {
        res = "-"
    }
    return fmt.Sprintf("[%s] %s %s : %v", e.Phase, e.Kind, res, e.Err)
}

// Stats agrège les compteurs par type de ressource.
type Stats struct {
    Found   int
    Deleted int
    Failed  int
    Skipped int
}

/* -------------------------------------------------------------------------- */
/*                                  Cleaner                                    */
/* -------------------------------------------------------------------------- */

// Clients regroupe les service clients OpenStack utilisés.
type Clients struct {
    Identity *gophercloud.ServiceClient
    Compute  *gophercloud.ServiceClient
    Network  *gophercloud.ServiceClient
    Volume   *gophercloud.ServiceClient
    Image    *gophercloud.ServiceClient
    Share    *gophercloud.ServiceClient // optionnel (Manila peut être absent)
}

// Cleaner orchestre le listing puis la suppression des ressources.
type Cleaner struct {
    log       *slog.Logger
    clients   *Clients
    projectID string
    projName  string

    dryRun  bool
    timeout time.Duration

    inventory map[ResourceKind][]Resource
    stats     map[ResourceKind]*Stats
    errs      []OpError
    mu        sync.Mutex
}

func NewCleaner(log *slog.Logger, cl *Clients, projectID, projName string, dryRun bool, timeout time.Duration) *Cleaner {
    return &Cleaner{
        log:       log,
        clients:   cl,
        projectID: projectID,
        projName:  projName,
        dryRun:    dryRun,
        timeout:   timeout,
        inventory: make(map[ResourceKind][]Resource),
        stats:     make(map[ResourceKind]*Stats),
    }
}

func (cl *Cleaner) stat(k ResourceKind) *Stats {
    if _, ok := cl.stats[k]; !ok {
        cl.stats[k] = &Stats{}
    }
    return cl.stats[k]
}

func (cl *Cleaner) addErr(phase string, kind ResourceKind, resource string, err error) {
    cl.mu.Lock()
    defer cl.mu.Unlock()
    cl.errs = append(cl.errs, OpError{Phase: phase, Kind: kind, Resource: resource, Err: err})
}

/* --------------------------------- Listing -------------------------------- */

// Discover interroge tous les services et remplit l'inventaire.
func (cl *Cleaner) Discover() {
    section("PHASE 1 — INVENTAIRE DES RESSOURCES")

    type lister struct {
        kind ResourceKind
        fn   func() ([]Resource, error)
    }

    listers := []lister{
        {KindServer, cl.listServers},
        {KindSnapshot, cl.listSnapshots},
        {KindVolume, cl.listVolumes},
        {KindShare, cl.listShares},
        {KindImage, cl.listImages},
        {KindFloatingIP, cl.listFloatingIPs},
        {KindPort, cl.listPorts},
        {KindRouter, cl.listRouters},
        {KindNetwork, cl.listNetworks},
    }

    for _, l := range listers {
        cl.log.Debug("listing", "kind", string(l.kind))
        res, err := l.fn()
        if err != nil {
            cl.log.Error("échec du listing", "kind", string(l.kind), "error", err)
            cl.addErr("list", l.kind, "", err)
            fmt.Printf("  %s %-24s %s\n", symFail(), l.kind, c(colRed, "erreur: "+err.Error()))
            continue
        }
        cl.inventory[l.kind] = res
        cl.stat(l.kind).Found = len(res)

        fmt.Printf("\n%s %s %s\n", symInfo(), c(colBold, string(l.kind)),
            c(colGrey, fmt.Sprintf("— %d ressource(s)", len(res))))
        if len(res) == 0 {
            fmt.Printf("   %s\n", c(colGrey, "aucune ressource"))
            continue
        }
        for _, r := range res {
            fmt.Printf("   %s %s\n", symArrow(), r.label())
        }
    }
}
func (cl *Cleaner) listFloatingIPs() ([]Resource, error) {
    if cl.clients.Network == nil {
        return nil, errors.New("service network indisponible")
    }
    pages, err := floatingips.List(cl.clients.Network, floatingips.ListOpts{
        ProjectID: cl.projectID,
    }).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := floatingips.ExtractFloatingIPs(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, f := range list {
        extra := fmt.Sprintf("ip=%s", f.FloatingIP)
        if f.PortID != "" {
            extra += fmt.Sprintf(" port=%s fixed=%s", f.PortID, f.FixedIP)
        } else {
            extra += " (non associée)"
        }
        out = append(out, Resource{
            Kind:   KindFloatingIP,
            ID:     f.ID,
            Name:   f.FloatingIP, // pas de champ Name : on affiche l'adresse
            Status: f.Status,
            Extra:  extra,
        })
    }
    return out, nil
}

func (cl *Cleaner) listNetworks() ([]Resource, error) {
    if cl.clients.Network == nil {
        return nil, errors.New("service network indisponible")
    }
    pages, err := networks.List(cl.clients.Network, networks.ListOpts{
        ProjectID: cl.projectID,
    }).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := networks.ExtractNetworks(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, n := range list {
        // Les réseaux partagés/externes n'appartiennent pas vraiment au projet :
        // on ne tente jamais de les détruire.
        if n.Shared {
            cl.log.Debug("réseau partagé ignoré", "id", n.ID, "name", n.Name)
            continue
        }
        out = append(out, Resource{
            Kind:   KindNetwork,
            ID:     n.ID,
            Name:   n.Name,
            Status: n.Status,
            Extra:  fmt.Sprintf("subnets=%d", len(n.Subnets)),
        })
    }
    return out, nil
}
func (cl *Cleaner) listServers() ([]Resource, error) {
    if cl.clients.Compute == nil {
        return nil, errors.New("service compute indisponible")
    }
    pages, err := servers.List(cl.clients.Compute, servers.ListOpts{
        AllTenants: true,
        TenantID:   cl.projectID,
    }).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := servers.ExtractServers(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, s := range list {
        if s.TenantID != "" && s.TenantID != cl.projectID {
            continue
        }
        out = append(out, Resource{Kind: KindServer, ID: s.ID, Name: s.Name, Status: s.Status})
    }
    return out, nil
}

func (cl *Cleaner) listVolumes() ([]Resource, error) {
    if cl.clients.Volume == nil {
        return nil, errors.New("service block storage indisponible")
    }
    pages, err := volumes.List(cl.clients.Volume, volumes.ListOpts{
        AllTenants: true,
        TenantID:   cl.projectID,
    }).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := volumes.ExtractVolumes(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, v := range list {
        out = append(out, Resource{
            Kind:   KindVolume,
            ID:     v.ID,
            Name:   v.Name,
            Status: v.Status,
            Extra:  fmt.Sprintf("%dGio, %d attachement(s)", v.Size, len(v.Attachments)),
        })
    }
    return out, nil
}

func (cl *Cleaner) listSnapshots() ([]Resource, error) {
    if cl.clients.Volume == nil {
        return nil, errors.New("service block storage indisponible")
    }
    pages, err := snapshots.List(cl.clients.Volume, snapshots.ListOpts{
        AllTenants: true,
        TenantID:   cl.projectID,
    }).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := snapshots.ExtractSnapshots(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, s := range list {
        out = append(out, Resource{
            Kind:   KindSnapshot,
            ID:     s.ID,
            Name:   s.Name,
            Status: s.Status,
            Extra:  "volume=" + s.VolumeID,
        })
    }
    return out, nil
}

func (cl *Cleaner) listImages() ([]Resource, error) {
    if cl.clients.Image == nil {
        return nil, errors.New("service image indisponible")
    }
    pages, err := images.List(cl.clients.Image, images.ListOpts{Owner: cl.projectID}).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := images.ExtractImages(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, im := range list {
        // sécurité : ne jamais toucher aux images publiques/partagées d'un autre owner
        if im.Owner != cl.projectID {
            continue
        }
        out = append(out, Resource{
            Kind:   KindImage,
            ID:     im.ID,
            Name:   im.Name,
            Status: string(im.Status),
            Extra:  "visibility=" + string(im.Visibility),
        })
    }
    return out, nil
}

func (cl *Cleaner) listShares() ([]Resource, error) {
    if cl.clients.Share == nil {
        cl.log.Warn("service Manila (sharedfilesystems) non disponible — étape ignorée")
        return nil, nil
    }
    pages, err := shares.ListDetail(cl.clients.Share, shares.ListOpts{
        AllTenants: true,
        ProjectID:  cl.projectID,
    }).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := shares.ExtractShares(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, s := range list {
        out = append(out, Resource{
            Kind:   KindShare,
            ID:     s.ID,
            Name:   s.Name,
            Status: s.Status,
            Extra:  fmt.Sprintf("%dGio %s", s.Size, s.ShareProto),
        })
    }
    return out, nil
}

func (cl *Cleaner) listPorts() ([]Resource, error) {
    if cl.clients.Network == nil {
        return nil, errors.New("service network indisponible")
    }
    pages, err := ports.List(cl.clients.Network, ports.ListOpts{ProjectID: cl.projectID}).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := ports.ExtractPorts(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, p := range list {
        // Les ports possédés par un routeur (interface ou gateway) sont gérés
        // exclusivement lors de la suppression du routeur lui-même (cf. deleteRouter),
        // afin de respecter l'ordre : détachement subnet -> clear gateway -> delete router.
        if strings.HasPrefix(p.DeviceOwner, "network:router_") ||
            p.DeviceOwner == "network:ha_router_replicated_interface" {
            cl.log.Debug("port de routeur exclu du listing générique (géré par deleteRouter)",
                "port_id", p.ID, "device_owner", p.DeviceOwner)
            continue
        }
        // Les ports DHCP sont gérés par Neutron lui-même à la suppression du réseau/subnet,
        // on les exclut aussi pour éviter des suppressions inutiles ou en échec.
        if p.DeviceOwner == "network:dhcp" {
            continue
        }

        ips := make([]string, 0, len(p.FixedIPs))
        for _, f := range p.FixedIPs {
            ips = append(ips, f.IPAddress)
        }
        owner := p.DeviceOwner
        if owner == "" {
            owner = "none"
        }
        out = append(out, Resource{
            Kind:   KindPort,
            ID:     p.ID,
            Name:   p.Name,
            Status: p.Status,
            Extra:  fmt.Sprintf("owner=%s ips=[%s]", owner, strings.Join(ips, ",")),
        })
    }
    return out, nil
}

func (cl *Cleaner) listRouters() ([]Resource, error) {
    if cl.clients.Network == nil {
        return nil, errors.New("service network indisponible")
    }
    pages, err := routers.List(cl.clients.Network, routers.ListOpts{ProjectID: cl.projectID}).AllPages()
    if err != nil {
        return nil, err
    }
    list, err := routers.ExtractRouters(pages)
    if err != nil {
        return nil, err
    }
    out := make([]Resource, 0, len(list))
    for _, r := range list {
        out = append(out, Resource{
            Kind:   KindRouter,
            ID:     r.ID,
            Name:   r.Name,
            Status: r.Status,
            Extra:  fmt.Sprintf("ha=%v", r.Distributed),
        })
    }
    return out, nil
}

/* ------------------------------- Suppression ------------------------------ */

// Cleanup supprime les ressources dans l'ordre de dépendance.
func (cl *Cleaner) Cleanup() {
    if cl.dryRun {
        section("PHASE 2 — SIMULATION DE SUPPRESSION (DRY-RUN)")
    } else {
        section("PHASE 2 — SUPPRESSION DES RESSOURCES")
    }

    for _, kind := range deletionOrder {
        res := cl.inventory[kind]
        if len(res) == 0 {
            continue
        }
        fmt.Printf("\n%s %s %s\n", symInfo(), c(colBold, string(kind)),
            c(colGrey, fmt.Sprintf("— %d à supprimer", len(res))))

        for _, r := range res {
            if cl.dryRun {
                cl.stat(kind).Skipped++
                fmt.Printf("   %s %s %s\n", symDry(), c(colYellow, "SERAIT SUPPRIMÉ"), r.label())
                cl.log.Info("dry-run", "kind", string(kind), "id", r.ID, "name", r.Name)
                continue
            }
            if err := cl.deleteOne(r); err != nil {
                cl.stat(kind).Failed++
                cl.addErr("delete", kind, fmt.Sprintf("%s (%s)", r.Name, r.ID), err)
                cl.log.Error("suppression échouée", "kind", string(kind), "id", r.ID, "error", err)
                fmt.Printf("   %s %s %s\n", symFail(), r.label(), c(colRed, "— "+err.Error()))
                continue
            }
            cl.stat(kind).Deleted++
            cl.log.Info("ressource supprimée", "kind", string(kind), "id", r.ID, "name", r.Name)
            fmt.Printf("   %s %s\n", symOK(), r.label())
        }

        // Laisse le temps au backend de libérer les dépendances
        if !cl.dryRun && (kind == KindServer || kind == KindSnapshot ||
            kind == KindFloatingIP || kind == KindRouter) {
            cl.waitSettle(kind)
        }
    }
}

// deleteOne aiguille vers l'appel API correspondant au type de ressource.
func (cl *Cleaner) deleteOne(r Resource) error {
    switch r.Kind {

    case KindServer:
        return servers.Delete(cl.clients.Compute, r.ID).ExtractErr()

    case KindSnapshot:
        return snapshots.Delete(cl.clients.Volume, r.ID).ExtractErr()

    case KindVolume:
        // Un volume "in-use" doit d'abord être détaché : on tente le force delete.
        err := volumes.Delete(cl.clients.Volume, r.ID, volumes.DeleteOpts{Cascade: true}).ExtractErr()
        if err != nil && isConflict(err) {
            cl.log.Warn("volume occupé, tentative de force delete", "id", r.ID)
            if ferr := volumeForceDelete(cl.clients.Volume, r.ID); ferr != nil {
                return fmt.Errorf("%w (force delete: %v)", err, ferr)
            }
            return nil
        }
        return err

    case KindShare:
        if cl.clients.Share == nil {
            return errors.New("service Manila indisponible")
        }
        return shares.Delete(cl.clients.Share, r.ID).ExtractErr()

    case KindImage:
        return images.Delete(cl.clients.Image, r.ID).ExtractErr()

    case KindPort:
        // Les ports gérés par l'infra (router, dhcp) sont détachés puis supprimés.
        if strings.HasPrefix(extractOwner(r.Extra), "network:router_interface") {
            cl.log.Debug("port d'interface routeur : suppression via le routeur", "id", r.ID)
        }
        return ports.Delete(cl.clients.Network, r.ID).ExtractErr()

    case KindRouter:
        return cl.deleteRouter(r.ID)

    case KindFloatingIP:
        // Si la FIP est associée, on la dissocie d'abord (PortID vide),
        // sinon Neutron refuse parfois la suppression directe.
        if strings.Contains(r.Extra, "port=") {
            empty := ""
            if _, uerr := floatingips.Update(cl.clients.Network, r.ID,
                floatingips.UpdateOpts{PortID: &empty}).Extract(); uerr != nil && !isNotFound(uerr) {
                cl.log.Warn("dissociation de la floating IP échouée, suppression directe",
                    "id", r.ID, "error", uerr)
            }
        }
        return floatingips.Delete(cl.clients.Network, r.ID).ExtractErr()

    case KindNetwork:
        return cl.deleteNetwork(r.ID)
    }
    return fmt.Errorf("type de ressource non pris en charge: %s", r.Kind)
}

// deleteNetwork supprime les subnets du réseau puis le réseau lui-même.
func (cl *Cleaner) deleteNetwork(networkID string) error {
    // 1. Purger les ports résiduels (agents DHCP, ports orphelins).
    pages, err := ports.List(cl.clients.Network, ports.ListOpts{
        NetworkID: networkID,
    }).AllPages()
    if err != nil {
        cl.log.Warn("listing des ports du réseau impossible", "network_id", networkID, "error", err)
    } else {
        netPorts, eerr := ports.ExtractPorts(pages)
        if eerr != nil {
            cl.log.Warn("extraction des ports du réseau impossible", "network_id", networkID, "error", eerr)
        }
        for _, p := range netPorts {
            // Les interfaces de routeur sont gérées par deleteRouter ; on ignore.
            if strings.HasPrefix(p.DeviceOwner, "network:router_") {
                continue
            }
            cl.log.Debug("suppression port résiduel", "network_id", networkID, "port_id", p.ID)
            if derr := ports.Delete(cl.clients.Network, p.ID).ExtractErr(); derr != nil && !isNotFound(derr) {
                cl.log.Warn("suppression du port résiduel échouée", "port_id", p.ID, "error", derr)
            }
        }
    }

    // 2. Supprimer les subnets.
    spages, err := subnets.List(cl.clients.Network, subnets.ListOpts{
        NetworkID: networkID,
    }).AllPages()
    if err != nil {
        return fmt.Errorf("listing des subnets: %w", err)
    }
    subnetList, err := subnets.ExtractSubnets(spages)
    if err != nil {
        return fmt.Errorf("extraction des subnets: %w", err)
    }
    for _, s := range subnetList {
        cl.log.Debug("suppression subnet", "network_id", networkID, "subnet_id", s.ID)
        if derr := subnets.Delete(cl.clients.Network, s.ID).ExtractErr(); derr != nil && !isNotFound(derr) {
            return fmt.Errorf("suppression du subnet %s (%s): %w", s.Name, s.ID, derr)
        }
    }

    // 3. Supprimer le réseau.
    if derr := networks.Delete(cl.clients.Network, networkID).ExtractErr(); derr != nil && !isNotFound(derr) {
        return derr
    }
    return nil
}
func (cl *Cleaner) deleteRouter(routerID string) error {
    // 1. Lister les ports attachés à ce routeur (interfaces de subnets)
    pages, err := ports.List(cl.clients.Network, ports.ListOpts{
        DeviceID: routerID,
    }).AllPages()
    if err != nil {
        return fmt.Errorf("listing des ports du routeur: %w", err)
    }
    routerPorts, err := ports.ExtractPorts(pages)
    if err != nil {
        return fmt.Errorf("extraction des ports du routeur: %w", err)
    }

    for _, p := range routerPorts {
        switch p.DeviceOwner {
        case "network:router_interface", "network:router_interface_distributed", "network:ha_router_replicated_interface":
            // Il faut retirer l'interface pour CHAQUE subnet du port (généralement un seul).
            for _, fip := range p.FixedIPs {
                subnetID := fip.SubnetID
                cl.log.Debug("détachement interface routeur",
                    "router_id", routerID, "subnet_id", subnetID, "port_id", p.ID)

                _, err := routers.RemoveInterface(cl.clients.Network, routerID, routers.RemoveInterfaceOpts{
                    SubnetID: subnetID,
                }).Extract()
                if err != nil && !isNotFound(err) {
                    return fmt.Errorf("détachement subnet %s du routeur %s: %w", subnetID, routerID, err)
                }
            }
        case "network:router_gateway":
            // Ce port est géré par clear_gateway, pas par RemoveInterface.
            continue
        default:
            cl.log.Warn("port inattendu attaché au routeur, ignoré",
                "router_id", routerID, "port_id", p.ID, "device_owner", p.DeviceOwner)
        }
    }

    // 2. Détacher la gateway externe (équivalent openstack router unset --external-gateway)
    cl.log.Debug("détachement de la gateway externe", "router_id", routerID)
    _, err = routers.Update(cl.clients.Network, routerID, routers.UpdateOpts{
        GatewayInfo: &routers.GatewayInfo{}, // struct vide == clear_gateway côté API Neutron
    }).Extract()
    if err != nil && !isNotFound(err) {
        return fmt.Errorf("détachement gateway du routeur %s: %w", routerID, err)
    }

    // 3. Supprimer le routeur
    if err := routers.Delete(cl.clients.Network, routerID).ExtractErr(); err != nil {
        return fmt.Errorf("suppression du routeur %s: %w", routerID, err)
    }
    return nil
}

func isNotFound(err error) bool {
    var e404 gophercloud.ErrDefault404
    return errors.As(err, &e404)
}

// volumeForceDelete appelle l'action admin os-force_delete.
func volumeForceDelete(client *gophercloud.ServiceClient, id string) error {
    body := map[string]interface{}{"os-force_delete": struct{}{}}
    _, err := client.Post(client.ServiceURL("volumes", id, "action"), body, nil,
        &gophercloud.RequestOpts{OkCodes: []int{202}})
    return err
}

func isConflict(err error) bool {
    var e409 gophercloud.ErrDefault409
    var e400 gophercloud.ErrDefault400
    return errors.As(err, &e409) || errors.As(err, &e400)
}

func extractOwner(extra string) string {
    for _, tok := range strings.Fields(extra) {
        if strings.HasPrefix(tok, "owner=") {
            return strings.TrimPrefix(tok, "owner=")
        }
    }
    return ""
}

// waitSettle laisse un délai de propagation entre deux catégories dépendantes.
func (cl *Cleaner) waitSettle(kind ResourceKind) {
    d := 5 * time.Second
    fmt.Printf("   %s %s\n", symInfo(),
        c(colGrey, fmt.Sprintf("attente de %s pour la libération des dépendances (%s)…", d, kind)))
    time.Sleep(d)
}

/* --------------------------------- Rapport -------------------------------- */

func (cl *Cleaner) Report() int {
    section("PHASE 3 — RÉSUMÉ")

    fmt.Printf("\nProjet : %s %s\n", c(colBold, cl.projName), c(colGrey, "("+cl.projectID+")"))
    mode := c(colYellow, "DRY-RUN (aucune modification)")
    if !cl.dryRun {
        mode = c(colRed, "PRODUCTION (suppressions réelles)")
    }
    fmt.Printf("Mode   : %s\n\n", mode)

    fmt.Printf("%-26s %8s %10s %8s %8s\n", "TYPE", "TROUVÉ", "SUPPRIMÉ", "ÉCHEC", "SIMULÉ")
    fmt.Println(strings.Repeat("─", 66))

    kinds := make([]ResourceKind, 0, len(cl.stats))
    for k := range cl.stats {
        kinds = append(kinds, k)
    }
    sort.Slice(kinds, func(i, j int) bool {
        return orderIndex(kinds[i]) < orderIndex(kinds[j])
    })

    var tf, td, tx, ts int
    for _, k := range kinds {
        s := cl.stats[k]
        tf, td, tx, ts = tf+s.Found, td+s.Deleted, tx+s.Failed, ts+s.Skipped
        line := fmt.Sprintf("%-26s %8d %10d %8d %8d", k, s.Found, s.Deleted, s.Failed, s.Skipped)
        if s.Failed > 0 {
            line = c(colRed, line)
        }
        fmt.Println(line)
    }
    fmt.Println(strings.Repeat("─", 66))
    fmt.Printf("%s\n", c(colBold, fmt.Sprintf("%-26s %8d %10d %8d %8d", "TOTAL", tf, td, tx, ts)))

    // Rapport d'erreurs
    fmt.Println()
    if len(cl.errs) == 0 {
        fmt.Printf("%s %s\n", symOK(), c(colGreen, "Aucune erreur rencontrée."))
        return 0
    }

    fmt.Printf("%s %s\n", symWarn(), c(colBold+colRed, fmt.Sprintf("%d erreur(s) rencontrée(s) :", len(cl.errs))))
    for i, e := range cl.errs {
        fmt.Printf("   %s %2d. %s\n", symFail(), i+1, e.String())
    }
    fmt.Printf("\n%s %s\n", symInfo(),
        c(colGrey, "Relancez l'outil pour retenter les ressources en échec (dépendances tardives)."))
    return 1
}

func orderIndex(k ResourceKind) int {
    for i, o := range deletionOrder {
        if o == k {
            return i
        }
    }
    return 99
}

/* -------------------------------------------------------------------------- */
/*                          Initialisation OpenStack                           */
/* -------------------------------------------------------------------------- */

func buildClients(log *slog.Logger, cloud string) (*Clients, error) {
    opts := &clientconfig.ClientOpts{Cloud: cloud}

    provider, err := clientconfig.AuthenticatedClient(opts)
    if err != nil {
        return nil, fmt.Errorf("authentification OpenStack impossible: %w", err)
    }
    log.Info("authentification réussie", "endpoint", provider.IdentityEndpoint)

    region := os.Getenv("OS_REGION_NAME")
    eo := gophercloud.EndpointOpts{Region: region}

    cls := &Clients{}

    if cls.Identity, err = openstack.NewIdentityV3(provider, eo); err != nil {
        return nil, fmt.Errorf("client identity v3: %w", err)
    }
    if cls.Compute, err = openstack.NewComputeV2(provider, eo); err != nil {
        log.Warn("service compute indisponible", "error", err)
    }
    if cls.Network, err = openstack.NewNetworkV2(provider, eo); err != nil {
        log.Warn("service network indisponible", "error", err)
    }
    if cls.Volume, err = openstack.NewBlockStorageV3(provider, eo); err != nil {
        log.Warn("service block storage v3 indisponible", "error", err)
    }
    if cls.Image, err = openstack.NewImageServiceV2(provider, eo); err != nil {
        log.Warn("service image indisponible", "error", err)
    }
    if cls.Share, err = openstack.NewSharedFileSystemV2(provider, eo); err != nil {
        log.Warn("service Manila indisponible (optionnel)", "error", err)
        cls.Share = nil
    } else {
        cls.Share.Microversion = "2.51"
    }

    return cls, nil
}

// resolveProject convertit un nom de projet en ID via Keystone.
func resolveProject(client *gophercloud.ServiceClient, name string) (id, resolved string, err error) {
    pages, err := projects.List(client, projects.ListOpts{Name: name}).AllPages()
    if err != nil {
        return "", "", fmt.Errorf("recherche du projet %q: %w", name, err)
    }
    list, err := projects.ExtractProjects(pages)
    if err != nil {
        return "", "", err
    }
    switch len(list) {
    case 0:
        // Peut-être un ID a-t-il été fourni directement.
        p, gerr := projects.Get(client, name).Extract()
        if gerr != nil {
            return "", "", fmt.Errorf("projet %q introuvable (ni par nom, ni par ID)", name)
        }
        return p.ID, p.Name, nil
    case 1:
        return list[0].ID, list[0].Name, nil
    default:
        ids := make([]string, 0, len(list))
        for _, p := range list {
            ids = append(ids, p.ID)
        }
        return "", "", fmt.Errorf("plusieurs projets nommés %q : %s — précisez l'ID",
            name, strings.Join(ids, ", "))
    }
}

/* -------------------------------------------------------------------------- */
/*                                    UI                                       */
/* -------------------------------------------------------------------------- */

func section(title string) {
    fmt.Printf("\n%s\n", c(colBold+colCyan, strings.Repeat("═", 66)))
    fmt.Printf("%s\n", c(colBold+colCyan, "  "+title))
    fmt.Printf("%s\n", c(colBold+colCyan, strings.Repeat("═", 66)))
}

// confirm demande une double confirmation avant destruction réelle.
// askProject / askDelete permettent de désactiver indépendamment chaque étape.
func confirm(projectName string, total int, askProject, askDelete bool) bool {
    fmt.Printf("\n%s %s\n", symWarn(),
        c(colBold+colRed, "ATTENTION : MODE PRODUCTION — SUPPRESSION IRRÉVERSIBLE"))
    fmt.Printf("%s %d ressource(s) du projet %s vont être définitivement supprimées.\n",
        symInfo(), total, c(colBold, projectName))

    if !askProject && !askDelete {
        fmt.Printf("%s %s\n", symWarn(),
            c(colYellow, "Confirmations désactivées par les options — poursuite immédiate."))
        return true
    }

    if askProject {
        fmt.Printf("\nPour confirmer, tapez exactement le nom du projet (%s) : ",
            c(colYellow, projectName))
        var answer string
        if _, err := fmt.Scanln(&answer); err != nil {
            return false
        }
        if strings.TrimSpace(answer) != projectName {
            return false
        }
    } else {
        fmt.Printf("%s %s\n", symWarn(),
            c(colYellow, "Saisie du nom du projet ignorée (--skip-project-prompt)."))
    }

    if askDelete {
        fmt.Printf("Confirmez une dernière fois en tapant %s : ", c(colYellow, "SUPPRIMER"))
        var second string
        if _, err := fmt.Scanln(&second); err != nil {
            return false
        }
        return strings.TrimSpace(second) == "SUPPRIMER"
    }

    fmt.Printf("%s %s\n", symWarn(),
        c(colYellow, "Saisie de SUPPRIMER ignorée (--skip-delete-prompt)."))
    return true
}

/* -------------------------------------------------------------------------- */
/*                                    Main                                     */
/* -------------------------------------------------------------------------- */

func main() {
	
    var (
        project = flag.String("project", "", "Nom (ou ID) du projet OpenStack à nettoyer — obligatoire")
        cloud   = flag.String("cloud", os.Getenv("OS_CLOUD"), "Entrée clouds.yaml à utiliser (défaut: $OS_CLOUD)")
        dryRun  = flag.Bool("dry-run", true, "Mode simulation : n'effectue aucune suppression (défaut: true)")
        yes     = flag.Bool("yes", false, "Ne pas demander de confirmation interactive (CI/CD)")
        force   = flag.Bool("force", false, "Alias de --yes : supprime sans aucune confirmation")
    
        skipProjectPrompt = flag.Bool("skip-project-prompt", false,
            "Ne pas demander de retaper le nom du projet")
        skipDeletePrompt = flag.Bool("skip-delete-prompt", false,
            "Ne pas demander de taper SUPPRIMER")
    
        verbose = flag.Bool("verbose", false, "Active les logs de niveau DEBUG")
        noColor = flag.Bool("no-color", false, "Désactive les couleurs ANSI")
        timeout = flag.Duration("timeout", 10*time.Minute, "Timeout global des appels API")
    )

    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, "openstack-cleaner — nettoyage des ressources d'un projet OpenStack\n\n")
        fmt.Fprintf(os.Stderr, "Usage:\n  %s --project <nom> [options]\n\nOptions:\n", os.Args[0])
        flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nExemples:\n")
        fmt.Fprintf(os.Stderr, "  OS_CLOUD=prod %s --project sandbox\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "  OS_CLOUD=prod %s --project sandbox --dry-run=false\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "  OS_CLOUD=prod %s --project sandbox --dry-run=false --skip-delete-prompt\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "  OS_CLOUD=prod %s --project sandbox --dry-run=false --yes   # CI/CD\n", os.Args[0])
    }
    flag.Parse()

    // Le nom du projet peut aussi être passé en argument positionnel.
    if *project == "" && flag.NArg() > 0 {
        *project = flag.Arg(0)
    }

    useColor = !*noColor && os.Getenv("NO_COLOR") == ""

    level := slog.LevelInfo
    if *verbose {
        level = slog.LevelDebug
    }
    log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})).
        With("app", "openstack-cleaner")

    if *project == "" {
        fmt.Fprintf(os.Stderr, "%s %s\n\n", symFail(), c(colRed, "le nom du projet est obligatoire"))
        flag.Usage()
        os.Exit(2)
    }
    if *cloud == "" {
        fmt.Fprintf(os.Stderr, "%s %s\n", symFail(),
            c(colRed, "OS_CLOUD n'est pas défini (ou utilisez --cloud)"))
        os.Exit(2)
    }

    section("OPENSTACK PROJECT CLEANER")
    fmt.Printf("%s cloud   : %s\n", symInfo(), *cloud)
    fmt.Printf("%s projet  : %s\n", symInfo(), *project)
    if *dryRun {
        fmt.Printf("%s mode    : %s\n", symInfo(), c(colYellow, "DRY-RUN (aucune suppression)"))
    } else {
        fmt.Printf("%s mode    : %s\n", symInfo(), c(colRed+colBold, "PRODUCTION"))
    }

    clients, err := buildClients(log, *cloud)
    if err != nil {
        log.Error("initialisation impossible", "error", err)
        fmt.Fprintf(os.Stderr, "\n%s %s\n", symFail(), c(colRed, err.Error()))
        os.Exit(1)
    }

    projectID, projectName, err := resolveProject(clients.Identity, *project)
    if err != nil {
        log.Error("résolution du projet impossible", "error", err)
        fmt.Fprintf(os.Stderr, "\n%s %s\n", symFail(), c(colRed, err.Error()))
        os.Exit(1)
    }
    log.Info("projet résolu", "name", projectName, "id", projectID)

    cleaner := NewCleaner(log, clients, projectID, projectName, *dryRun, *timeout)

    // Phase 1 : inventaire
    cleaner.Discover()

    total := 0
    for _, res := range cleaner.inventory {
        total += len(res)
    }
    if total == 0 {
        fmt.Printf("\n%s %s\n", symOK(), c(colGreen, "Aucune ressource à supprimer : le projet est déjà propre."))
        os.Exit(cleaner.Report())
    }

    // Confirmation en mode production
    noPrompt := *yes || *force
    if !*dryRun && !noPrompt {
        if !confirm(projectName, total, !*skipProjectPrompt, !*skipDeletePrompt) {
            fmt.Printf("\n%s %s\n", symWarn(), c(colYellow, "Confirmation invalide — opération annulée."))
            log.Warn("suppression annulée par l'utilisateur")
            os.Exit(130)
        }
    }

    // Phase 2 : suppression
    cleaner.Cleanup()

    // Phase 3 : rapport final
    os.Exit(cleaner.Report())
}
