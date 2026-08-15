package aether

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestLiveAppServerSmoke(t *testing.T) {
	if os.Getenv("AETHER_LIVE") != "1" {
		t.Skip("set AETHER_LIVE=1 to run the live App Server smoke")
	}
	version, err := exec.Command("codex", "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("codex --version: %v: %s", err, version)
	}
	t.Logf("Codex version: %s", strings.TrimSpace(string(version)))

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := Start(ctx, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	thread, err := client.StartThread(ctx, ThreadOptions{
		CWD:            dir,
		ApprovalPolicy: "never",
		Sandbox:        "read-only",
		Ephemeral:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := thread.Run(ctx, TurnRequest{
		Input: []Input{{Type: "text", Text: "Reply with exactly: ok. Do not use tools."}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(result.FinalText) != "ok" {
		t.Fatalf("unexpected final text: %q", result.FinalText)
	}
}
