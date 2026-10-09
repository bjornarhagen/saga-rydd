//go:build darwin

package service

import "context"

func controlLoginLink(context.Context, InstallSpec, string, loginLinkHooks) (LoginLinkResult, error) {
	return LoginLinkResult{}, ErrSpec
}
