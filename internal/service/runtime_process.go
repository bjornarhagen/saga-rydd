//go:build darwin || linux

package service

import (
	"context"
	"os/exec"
	"time"
)

// Keep CommandContext's Process.Kill cancellation. Cmd.Wait can reap its child
// before joining the cancellation watcher; raw PID/group signals would bypass
// Process's protection against PID reuse. Cancellation covers the direct client
// only. WaitDelay bounds inherited output-pipe waits, not descendant lifetimes.
func serviceClientCommand(ctx context.Context, program string, args, env []string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, program, args...)
	if env != nil {
		cmd.Env = env
	}
	cmd.WaitDelay = 100 * time.Millisecond
	return cmd
}

// Keep process-start evidence on failures. Killing this client cannot recall a
// manager request it may already have sent. Neither stdout nor stderr from a
// failed child is returned as a positive observation or an error message.
func runRuntimeProcess(ctx context.Context, program string, args []string) (runtimeReply, error) {
	return runRuntimeProcessWithEnv(ctx, program, args, nil)
}

func runRuntimeProcessWithEnv(ctx context.Context, program string, args, env []string) (runtimeReply, error) {
	if err := ctx.Err(); err != nil {
		return runtimeReply{}, err
	}
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := serviceClientCommand(childCtx, program, args, env)
	stdout := &managerOutput{limit: 64 << 10, cancel: cancel}
	stderr := &managerOutput{limit: 16 << 10, cancel: cancel}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return runtimeReply{}, ctx.Err()
		}
		return runtimeReply{}, ErrManagerUnavailable
	}
	reply := runtimeReply{started: true}
	err := cmd.Wait()
	if ctx.Err() != nil {
		return reply, ctx.Err()
	}
	if stdout.exceeded || stderr.exceeded {
		return reply, ErrLifecycleBounds
	}
	if err != nil {
		return reply, ErrManagerUnavailable
	}
	reply.data = append([]byte{}, stdout.data.Bytes()...)
	return reply, nil
}
