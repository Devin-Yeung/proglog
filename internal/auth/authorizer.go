package auth

import (
	"fmt"

	"github.com/casbin/casbin/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

// NewAuthorizer creates a new Authorizer with the given model and policy files.
func NewAuthorizer(model, policy string) (*Authorizer, error) {
	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, err
	}
	return &Authorizer{enforcer: enforcer}, nil
}

// Authorize checks if the subject is allowed to perform the action on the object.
func (a *Authorizer) Authorize(subject, object, action string) error {
	allowed, err := a.enforcer.Enforce(subject, object, action)
	if err != nil {
		return status.Errorf(codes.Internal, "authorize request: %v", err)
	}
	if !allowed {
		msg := fmt.Sprintf("%s is not allowed to %s on %s", subject, action, object)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}
