package monitoring

import "regexp"

var (
	methodPattern = regexp.MustCompile(`^[A-Z][A-Z0-9]{0,15}$`)
	// pathPattern is the route template, not the request URL.
	// '$' is outside the alphabet: Grafana would interpolate it inside a dashboard query.
	pathPattern = regexp.MustCompile(`^(?:/(?:[A-Za-z0-9._~-]+|\{[A-Za-z_][A-Za-z0-9_]*(?:\.\.\.)?\}))+$`)
)

// ValidMethod reports whether method can be a registered route method.
func ValidMethod(method string) bool {
	return methodPattern.MatchString(method)
}

// ValidRoutePath reports whether path is a route template the registrar accepts.
func ValidRoutePath(path string) bool {
	return pathPattern.MatchString(path)
}
