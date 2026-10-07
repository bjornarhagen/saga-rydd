package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

// reviewHashes closes each saved reader before displaying a prompt. Numbers
// refer only to the frozen groups and members, never to a fresh ID lookup.
// The default preview is ephemeral. Saving requires both the --save-choice
// opt-in and an explicit completed save response after the exact preview.
func reviewHashes(ctx context.Context, paths config.Paths, in io.Reader, out io.Writer, saveChoice bool) error {
	display := &reviewOutput{writer: out}
	input := in
	if file, ok := in.(*os.File); ok {
		input = reviewFileInput{ctx: ctx, file: file}
	}
	lines := bufio.NewReaderSize(input, reviewInputLimit+1)

refreshGroups:
	for {
		frozen, err := loadHashReviewGroups(ctx, paths)
		if err != nil {
			return err
		}
		if len(frozen.Groups) == 0 {
			if err = printHashGroups(display, frozen); err != nil {
				return err
			}
			return endHashReview(ctx, display)
		}
		printHashReviewGroups(display, frozen, saveChoice)
	groups:
		for {
			line, err := readHashReviewResponse(ctx, lines, display, "Choose a group number, refresh, or quit: ")
			if errors.Is(err, io.EOF) || err == nil && line == "quit" {
				return endHashReview(ctx, display)
			}
			if err != nil {
				return err
			}
			if line == "refresh" {
				continue refreshGroups
			}
			numbers, err := parseReviewNumbers(line)
			if err != nil || len(numbers) != 1 || numbers[0] > len(frozen.Groups) {
				fmt.Fprintln(display, "\nChoose one group number from this frozen list, refresh, or quit. Nothing saved.")
				continue
			}
			group := frozen.Groups[numbers[0]-1]
			printHashReviewMembers(display, group, saveChoice)
			if hashReviewAvailableMembers(group) < 2 {
				printWrapped(display, "This group has fewer than two paths without repeated or conflicting saved identities. A role preview is unavailable. No alternative paths were chosen.", "")
				printHashReviewGroups(display, frozen, saveChoice)
				continue
			}
		keepers:
			for {
				line, err = readHashReviewResponse(ctx, lines, display, "Choose one keeper row, back, or quit: ")
				if errors.Is(err, io.EOF) || err == nil && line == "quit" {
					return endHashReview(ctx, display)
				}
				if err != nil {
					return err
				}
				if line == "back" {
					printHashReviewGroups(display, frozen, saveChoice)
					continue groups
				}
				numbers, err = parseReviewNumbers(line)
				if err != nil || len(numbers) != 1 || numbers[0] > len(group.Members) {
					fmt.Fprintln(display, "\nChoose exactly one available keeper row from this frozen group. Use back to return to the groups. Nothing saved.")
					continue
				}
				keeper := group.Members[numbers[0]-1]
				if !hashReviewMemberAvailable(keeper) {
					fmt.Fprintln(display, "\nThis row has a repeated or conflicting saved identity. It is unavailable for a role preview. No other keeper was chosen.")
					continue
				}
				fmt.Fprintf(display, "\nPossible keeper you selected: row %d\n  %q\n", numbers[0], string(keeper.PathBytes))
				for {
					line, err = readHashReviewResponse(ctx, lines, display, "Choose copy rows (for example 1,3), back, or quit: ")
					if errors.Is(err, io.EOF) || err == nil && line == "quit" {
						return endHashReview(ctx, display)
					}
					if err != nil {
						return err
					}
					if line == "back" {
						printHashReviewMembers(display, group, saveChoice)
						continue keepers
					}
					numbers, err = parseReviewNumbers(line)
					if err != nil || len(numbers) >= inventory.FileSampleTargetLimit {
						fmt.Fprintln(display, "\nChoose 1–19 unique available copy rows, separated by spaces or commas. Ranges and all are not accepted. Nothing saved.")
						continue
					}
					copies, err := selectHashReviewCopies(group, keeper, numbers)
					if err != nil {
						fmt.Fprintln(display, "\nCopy rows must be unique, available and different from the keeper. No other copies were chosen. Nothing saved.")
						continue
					}
					copyIDs := make([]string, len(copies))
					for i, member := range copies {
						copyIDs[i] = member.WorkID
					}
					preview, err := loadHashReviewPreview(ctx, paths, frozen.SelectionID, keeper.WorkID, copyIDs)
					if err != nil {
						return fmt.Errorf("cannot preview the frozen selection; nothing saved. Run review --hashes again to see saved evidence: %w", err)
					}
					if err = checkHashReviewPreview(frozen, group, keeper, copies, preview); err != nil {
						return err
					}
					if err = ctx.Err(); err != nil {
						return err
					}
					if err = printHashKeeperPreview(display, preview); err != nil {
						return err
					}
					fmt.Fprintf(display, "\nRecreate this exact saved preview:\n  %s hashes --preview %s --keeper %s %s\n", commandPrefix(paths), frozen.SelectionID, keeper.WorkID, strings.Join(copyIDs, " "))
					if display.err != nil {
						return fmt.Errorf("cannot display hash review; no decisions saved: %w", display.err)
					}
					if !saveChoice {
						return ctx.Err()
					}
					for {
						line, err = readHashReviewResponse(ctx, lines, display, "Type save to preserve this historical choice, back, or quit: ")
						if errors.Is(err, io.EOF) || err == nil && line == "quit" {
							return endHashReview(ctx, display)
						}
						if err != nil {
							return err
						}
						if line == "back" {
							printHashReviewMembers(display, group, saveChoice)
							continue keepers
						}
						if line != "save" {
							fmt.Fprintln(display, "\nNothing saved. Enter save, back, or quit.")
							continue
						}
						if display.err != nil {
							return fmt.Errorf("cannot display the frozen preview; nothing saved: %w", display.err)
						}
						saved, err := saveHashKeeperChoice(ctx, paths, preview)
						if err != nil {
							return err
						}
						if err = printHashKeeperChoice(display, saved); err != nil {
							return err
						}
						fmt.Fprintf(display, "\nReopen this exact historical choice:\n  %s hashes --choice %s\n", commandPrefix(paths), saved.ID)
						if display.err != nil {
							return fmt.Errorf("choice %s was saved, but its completion output failed; reopen with hashes --choice %s: %w", saved.ID, saved.ID, display.err)
						}
						if err = ctx.Err(); err != nil {
							return fmt.Errorf("choice %s was saved, but its reply was canceled; reopen with hashes --choice %s: %w", saved.ID, saved.ID, err)
						}
						return nil
					}
				}
			}
		}
	}
}

func loadHashReviewGroups(ctx context.Context, paths config.Paths) (inventory.HashGroupsReport, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reader, err := inventory.OpenHashReader(opCtx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("saved hash storage is unavailable; review --hashes only reads existing observations: %w", err)}
		}
		return inventory.HashGroupsReport{}, err
	}
	report, readErr := reader.Groups(opCtx)
	closeErr := reader.Close()
	if readErr != nil {
		return inventory.HashGroupsReport{}, readErr
	}
	if closeErr != nil {
		return inventory.HashGroupsReport{}, closeErr
	}
	if err = opCtx.Err(); err != nil {
		return inventory.HashGroupsReport{}, err
	}
	return report, nil
}

func loadHashReviewPreview(ctx context.Context, paths config.Paths, selectionID, keeperID string, copies []string) (inventory.HashKeeperPreview, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reader, err := inventory.OpenHashReader(opCtx, paths.StateDir)
	if err != nil {
		return inventory.HashKeeperPreview{}, err
	}
	preview, readErr := reader.PreviewKeeper(opCtx, selectionID, keeperID, copies)
	closeErr := reader.Close()
	if readErr != nil {
		return inventory.HashKeeperPreview{}, readErr
	}
	if closeErr != nil {
		return inventory.HashKeeperPreview{}, closeErr
	}
	if err = opCtx.Err(); err != nil {
		return inventory.HashKeeperPreview{}, err
	}
	return preview, nil
}

func hashReviewMemberAvailable(member inventory.SavedHashGroupMember) bool {
	return !member.RepeatedSavedIdentity && !member.SavedIdentityConflict
}

func hashReviewAvailableMembers(group inventory.SavedHashGroup) int {
	available := 0
	for _, member := range group.Members {
		if hashReviewMemberAvailable(member) {
			available++
		}
	}
	return available
}

func selectHashReviewCopies(group inventory.SavedHashGroup, keeper inventory.SavedHashGroupMember, numbers []int) ([]inventory.SavedHashGroupMember, error) {
	if len(numbers) < 1 || len(numbers) >= inventory.FileSampleTargetLimit {
		return nil, inventory.ErrHashKeeperRequest
	}
	copies := make([]inventory.SavedHashGroupMember, 0, len(numbers))
	seen := map[string]bool{keeper.WorkID: true}
	for _, number := range numbers {
		if number < 1 || number > len(group.Members) {
			return nil, inventory.ErrHashKeeperRequest
		}
		member := group.Members[number-1]
		if seen[member.WorkID] || !hashReviewMemberAvailable(member) {
			return nil, inventory.ErrHashKeeperRequest
		}
		seen[member.WorkID] = true
		copies = append(copies, member)
	}
	return copies, nil
}

func checkHashReviewPreview(frozen inventory.HashGroupsReport, group inventory.SavedHashGroup, keeper inventory.SavedHashGroupMember, copies []inventory.SavedHashGroupMember, preview inventory.HashKeeperPreview) error {
	if preview.StoreID != frozen.StoreID || preview.SelectionID != frozen.SelectionID || preview.InventoryID != frozen.InventoryID || preview.LogicalBytes != group.LogicalBytes || preview.SHA256 != group.SHA256 || !reflect.DeepEqual(preview.Keeper.SavedHashGroupMember, keeper) || len(preview.Copies) != len(copies) {
		return fmt.Errorf("saved hash evidence changed during review; nothing saved. Run review --hashes again: %w", inventory.ErrHashKeeperSelection)
	}
	for i, member := range copies {
		if !reflect.DeepEqual(preview.Copies[i].SavedHashGroupMember, member) {
			return fmt.Errorf("saved hash evidence changed during review; nothing saved. Run review --hashes again: %w", inventory.ErrHashKeeperSelection)
		}
	}
	return nil
}

func printHashReviewGroups(out io.Writer, frozen inventory.HashGroupsReport, saveChoice bool) {
	if saveChoice {
		printResultBanner(out, "REVIEW SAVED HISTORICAL HASH GROUPS - EXPLICIT SAVE AVAILABLE")
		printWrapped(out, "After the exact preview, type save to preserve your unapproved historical choice. No decision is saved before that response. No cleanup approval is available.", "")
	} else {
		printResultBanner(out, "REVIEW SAVED HISTORICAL HASH GROUPS - NOTHING WILL BE SAVED")
	}
	printWrapped(out, "These numbered groups are frozen saved full-hash observations. Current files have not been checked, and observations need not be simultaneous. Choose a group explicitly, even when only one is listed. No keeper, copy or cleanup choice is automatic.", "")
	fmt.Fprintf(out, "Store: %s\nSelection: %s\n", frozen.StoreID, frozen.SelectionID)
	printField(out, "Selected work", frozen.SelectedWork)
	printField(out, "Completed observations", frozen.CompletedObservations)
	printField(out, "Without a full observation", frozen.UnfinishedWork)
	printField(out, "Completed without a match", frozen.UnmatchedCompletedObservations)
	for i, group := range frozen.Groups {
		fmt.Fprintf(out, "\nGroup %d\n", i+1)
		printField(out, "Logical size per file", humanBytes(group.LogicalBytes))
		printField(out, "Historical SHA-256", group.SHA256)
		printField(out, "Recorded paths", len(group.Members))
		printField(out, "Available for role preview", hashReviewAvailableMembers(group))
	}
	printWrapped(out, "Coverage is limited to this saved selection. Work without a full observation includes pending, running and invalidated records; saved running state does not prove a process is active. refresh replaces this list and resets all choices. back retains the frozen list.", "")
}

func printHashReviewMembers(out io.Writer, group inventory.SavedHashGroup, saveChoice bool) {
	printResultBanner(out, "FROZEN GROUP ROWS - POSSIBLE ROLES ONLY")
	printField(out, "Logical size per file", humanBytes(group.LogicalBytes))
	for i, member := range group.Members {
		fmt.Fprintf(out, "\nRow %d; work %s; saved file %d; saved root %d\n  %q\n", i+1, member.WorkID, member.FileID, member.RootID, string(member.PathBytes))
		printField(out, "Historical check time", member.CheckedAt.UTC().Format(time.RFC3339Nano))
		printField(out, "Saved device / inode", member.SavedDevice+" / "+member.SavedInode)
		if !hashReviewMemberAvailable(member) {
			printField(out, "Role preview", "Unavailable: repeated or conflicting saved identity")
		}
	}
	printWrapped(out, "Choose one available keeper row, then explicitly choose other available rows as possible copies. Other matching paths are not selected. Saved identities do not prove current hardlinks, inode continuity or independent storage.", "")
	if saveChoice {
		printWrapped(out, "Saving requires a separate save response after the exact preview. Only historical roles can be saved; approval and reclaimable space remain unavailable.", "")
	} else {
		printWrapped(out, "No decision, approval or reclaimable-space estimate will be saved.", "")
	}
}

func readHashReviewResponse(ctx context.Context, lines *bufio.Reader, display *reviewOutput, prompt string) (string, error) {
	if display.err != nil {
		return "", fmt.Errorf("cannot display hash review; no decisions saved: %w", display.err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fmt.Fprintf(display, "\n%s", prompt)
	if display.err != nil {
		return "", fmt.Errorf("cannot display hash review; no decisions saved: %w", display.err)
	}
	line, err := readReviewLine(ctx, lines)
	if canceled := ctx.Err(); canceled != nil {
		return "", canceled
	}
	return line, err
}

func endHashReview(ctx context.Context, display *reviewOutput) error {
	fmt.Fprintln(display, "\nHash review ended. No decisions were saved. No source files or saved records were changed.")
	if display.err != nil {
		return fmt.Errorf("cannot display hash review completion; no decisions saved: %w", display.err)
	}
	return ctx.Err()
}
