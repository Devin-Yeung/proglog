package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

// NewAuthorizer creates a new Authorizer with the given model and policy files.
func NewAuthorizer(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{enforcer: enforcer}
}

// Authorize checks if the subject is allowed to perform the action on the object.
func (a *Authorizer) Authorize(subject, object, action string) error {
	allowed := a.enforcer.Enforce(subject, object, action)
	if !allowed {
		msg := fmt.Sprintf("%s is not allowed to %s on %s", subject, action, object)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}
