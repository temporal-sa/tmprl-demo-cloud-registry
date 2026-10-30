package kubernetes

import (
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	"k8s.io/apimachinery/pkg/util/validation"
)

type middlewareRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}
type routeService struct {
	Name           string `json:"name"`
	Namespace      string `json:"namespace"`
	Port           int32  `json:"port"`
	PassHostHeader bool   `json:"passHostHeader"`
}
type routeRule struct {
	Kind        string          `json:"kind"`
	Match       string          `json:"match"`
	Priority    int             `json:"priority"`
	Services    []routeService  `json:"services"`
	Middlewares []middlewareRef `json:"middlewares,omitempty"`
}
type routeTLS struct {
	SecretName string `json:"secretName,omitempty"`
}
type ingressSpec struct {
	EntryPoints []string    `json:"entryPoints"`
	Routes      []routeRule `json:"routes"`
	TLS         *routeTLS   `json:"tls,omitempty"`
}
type stripHeaders struct {
	CustomRequestHeaders map[string]string `json:"customRequestHeaders"`
}
type redirectScheme struct {
	Scheme    string `json:"scheme"`
	Permanent bool   `json:"permanent"`
}
type forwardAuth struct {
	Address             string   `json:"address"`
	AuthRequestHeaders  []string `json:"authRequestHeaders"`
	AuthResponseHeaders []string `json:"authResponseHeaders"`
}
type middlewareSpec struct {
	Headers        *stripHeaders   `json:"headers,omitempty"`
	RedirectScheme *redirectScheme `json:"redirectScheme,omitempty"`
	ForwardAuth    *forwardAuth    `json:"forwardAuth,omitempty"`
}

// RenderPromotion plans routing only. It does not claim to authorize promotion:
// the publisher must independently enforce candidate readiness, writer fencing,
// stable route UID/version or atomic absence and parent-namespace lifecycle.
func RenderPromotion(authored project.Document, b PromotionBindings) (Bundle, error) {
	document, err := project.Prepare(authored)
	if err != nil {
		return Bundle{}, renderError("InvalidProject", "project", "Project authoring validation failed; inspect the separate project validation issues.")
	}
	ingress := document.Spec.Ingress
	if ingress == nil {
		return Bundle{}, nil
	}
	if b.Namespace.NamespaceName() == "" || b.Namespace.Parent().UID == "" || b.Backend.Parent().UID == "" || b.Namespace.Owner() != b.Backend.Owner() || b.Namespace.Scope().ClusterID != b.Backend.Scope().ClusterID {
		return Bundle{}, renderError("PromotionAuthorityRequired", "ingress", "Traffic and backend require independently bound namespaces for the same owner and cluster.")
	}
	if document.Metadata.UID != "" && document.Metadata.UID != b.Namespace.Owner().ProjectUID {
		return Bundle{}, renderError("ProjectIdentityMismatch", "metadata.uid", "The promoted project UID differs from the verified routing owner.")
	}
	if b.Namespace.ParentPurpose() != "namespace" || b.Namespace.NamespaceName() == b.Backend.NamespaceName() {
		return Bundle{}, renderError("StableRoutingNamespaceRequired", "ingress", "Promotion must target a distinct verified stable routing namespace, not a candidate runtime namespace.")
	}
	if b.Routing == nil || b.Routing.NamespaceUID != b.Namespace.Parent().UID || b.Routing.BaseDomain != b.BaseDomain {
		return Bundle{}, renderError("RoutingDomainCapabilityRequired", "ingress", "The installation must explicitly bind routing authority to this namespace incarnation and base domain.")
	}
	if b.Revision == "" || b.OperationID == "" || b.Backend.ParentRevision() != b.Revision {
		return Bundle{}, renderError("CandidateNamespaceRevisionMismatch", "ingress", "The backend namespace ledger revision must match the promoted candidate.")
	}
	if b.Namespace.NamespaceName() != b.Backend.NamespaceName() && !b.AllowCrossNamespaceBackend {
		return Bundle{}, renderError("CrossNamespaceRoutingApprovalRequired", "ingress", "The installation must explicitly approve cross-namespace backend references.")
	}
	if b.BaseDomain == "" || len(validation.IsDNS1123Subdomain(b.BaseDomain)) > 0 {
		return Bundle{}, renderError("BaseDomainBindingRequired", "ingress", "A valid trusted base domain must be supplied explicitly.")
	}
	host := string(ingress.Subdomain) + "." + b.BaseDomain
	if len(validation.IsDNS1123Subdomain(host)) > 0 {
		return Bundle{}, renderError("InvalidIngressHost", "ingress", "The requested host exceeds valid DNS bounds.")
	}
	authRequired := ingress.TemporalAuthRequired == nil || *ingress.TemporalAuthRequired
	if !authRequired && !b.AllowPublicIngress {
		return Bundle{}, renderError("PublicIngressApprovalRequired", "ingress", "Disabling authentication requires explicit installation approval.")
	}
	if authRequired {
		address, err := url.Parse(b.AuthURL)
		if err != nil || address.Host == "" || (address.Scheme != "http" && address.Scheme != "https") || address.User != nil || address.RawQuery != "" || address.Fragment != "" {
			return Bundle{}, renderError("AuthEndpointBindingRequired", "ingress.auth", "Protected ingress requires an explicit credential-free trusted authentication endpoint.")
		}
	}
	if b.UseDefaultTLSStore && b.TLSSecret != nil {
		return Bundle{}, renderError("AmbiguousTLSBinding", "ingress.tls", "Choose either the approved default TLS store or one local TLS Secret.")
	}
	if !b.UseDefaultTLSStore && b.TLSSecret == nil {
		return Bundle{}, renderError("TLSBindingRequired", "ingress.tls", "TLS requires an explicitly approved default store or local certificate Secret.")
	}
	bindings := Bindings{Namespace: b.Namespace, Revision: b.Revision, OperationID: b.OperationID}
	bundle := Bundle{}
	requirements := map[string]SecretRequirement{}
	tls := &routeTLS{}
	if b.TLSSecret != nil {
		for _, key := range []string{"tls.crt", "tls.key"} {
			if b.TLSSecret.Keys[key] != key {
				return Bundle{}, renderError("TLSSecretShape", "ingress.tls", "A TLS Secret binding must expose the standard certificate and key properties.")
			}
			if err = requireSecret(*b.TLSSecret, key, bindings, requirements); err != nil {
				return Bundle{}, err
			}
		}
		tls.SecretName = b.TLSSecret.Name
	}
	namespace, parentUID, owner := b.Namespace.NamespaceName(), b.Namespace.Parent().UID, b.Namespace.Owner()
	add := func(name, kind, purpose string, spec any) error {
		object, err := custom("traefik.io/v1alpha1", kind, metadata(name, namespace, parentUID, owner, document, "", bindings), spec)
		if err != nil {
			return err
		}
		item, err := desired(object, purpose, bindings)
		if err != nil {
			return err
		}
		bundle.Objects = append(bundle.Objects, item)
		return nil
	}
	identityHeaders := []string{"X-Temporal-Auth-JWT", "X-Temporal-Auth-Subject", "X-Temporal-Auth-Email", "X-Temporal-Auth-Name"}
	headers := map[string]string{}
	for _, key := range identityHeaders {
		headers[key] = ""
	}
	// Strip untrusted identity headers even when the application explicitly opts
	// out of SSO; a public endpoint never gains trusted employee identity.
	if err = add("temporal-auth-strip-headers", "Middleware", "middleware", middlewareSpec{Headers: &stripHeaders{CustomRequestHeaders: headers}}); err != nil {
		return Bundle{}, err
	}
	if err = add("https-redirect", "Middleware", "middleware", middlewareSpec{RedirectScheme: &redirectScheme{Scheme: "https", Permanent: true}}); err != nil {
		return Bundle{}, err
	}
	if authRequired {
		if err = add("temporal-auth-required", "Middleware", "middleware", middlewareSpec{ForwardAuth: &forwardAuth{Address: b.AuthURL, AuthRequestHeaders: []string{"Cookie", "X-Forwarded-Host", "X-Forwarded-Method", "X-Forwarded-Proto", "X-Forwarded-Uri"}, AuthResponseHeaders: identityHeaders}}); err != nil {
			return Bundle{}, err
		}
	}
	var secure, insecure []routeRule
	for _, route := range ingress.Routes {
		public := route.Public != nil && *route.Public
		if public && !b.AllowPublicIngress {
			return Bundle{}, renderError("PublicIngressApprovalRequired", "ingress.routes.public", "Public path exceptions require explicit installation approval.")
		}
		prefix := ""
		if route.PathPrefix != nil {
			prefix = *route.PathPrefix
		}
		if strings.ContainsAny(prefix, "\""+string(rune(96))+"\\") || strings.ContainsFunc(prefix, unicode.IsControl) {
			return Bundle{}, renderError("UnsafeIngressRule", "ingress.routes.pathPrefix", "Path prefixes cannot contain rule delimiters or control characters.")
		}
		match := "Host(" + strconv.Quote(host) + ")"
		if prefix != "" {
			match += " && PathPrefix(" + strconv.Quote(prefix) + ")"
		}
		component := document.Spec.Components[string(route.Component)]
		serviceName := string(route.Component)
		if component.ServiceName != nil {
			serviceName = string(*component.ServiceName)
		}
		port := int32(0)
		for _, value := range component.Ports {
			if value.Name != nil && *value.Name == route.Port {
				port = int32(*value.ServicePort)
			}
		}
		if port == 0 {
			return Bundle{}, renderError("InvalidIngressBackend", "ingress.routes", "The route has no normalized Kubernetes Service port.")
		}
		passHost := route.PassHostHeader == nil || *route.PassHostHeader
		rule := routeRule{Kind: "Rule", Match: match, Priority: 1000 + len(prefix), Services: []routeService{{Name: serviceName, Namespace: b.Backend.NamespaceName(), Port: port, PassHostHeader: passHost}}, Middlewares: []middlewareRef{{Name: "temporal-auth-strip-headers", Namespace: namespace}}}
		if authRequired && !public {
			rule.Middlewares = append(rule.Middlewares, middlewareRef{Name: "temporal-auth-required", Namespace: namespace})
		}
		secure = append(secure, rule)
		http := rule
		http.Middlewares = []middlewareRef{{Name: "https-redirect", Namespace: namespace}}
		insecure = append(insecure, http)
	}
	if err = add(document.Metadata.Name, "IngressRoute", "ingress-route", ingressSpec{EntryPoints: []string{"websecure"}, Routes: secure, TLS: tls}); err != nil {
		return Bundle{}, err
	}
	if err = add(document.Metadata.Name+"-http", "IngressRoute", "ingress-route", ingressSpec{EntryPoints: []string{"web"}, Routes: insecure}); err != nil {
		return Bundle{}, err
	}
	for _, name := range keys(requirements) {
		bundle.RequiredSecrets = append(bundle.RequiredSecrets, requirements[name])
	}
	return bundle, nil
}
