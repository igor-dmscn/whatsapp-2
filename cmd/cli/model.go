package main

import (
	"context"
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"comms/internal/client"
	"comms/internal/platform/id"
)

// Messages the sync goroutine sends the interface.
type (
	storeChanged struct{}
	syncFailed   struct{ err error }
	loaded       struct {
		conversations []client.LocalConversation
		entries       []client.LocalEntry
		reactions     map[int64]map[string]int
	}
	searched struct{ results []client.SearchResult }
	failed   struct{ err error }
)

// view is which screen is showing.
type view int

const (
	viewConversations view = iota
	viewConversation
	viewSearch
)

// model is the whole interface.
//
// Everything it draws comes from the local store, never from a network call. That is
// the phase-6 change stated as a design rule: the store is the source the UI reads, and
// syncing fills it from behind.
type model struct {
	ctx    context.Context //nolint:containedctx // Bubble Tea's Update carries no context.
	api    *client.API
	store  *client.Store
	syncer *client.Syncer

	view     view
	selected int
	// open is the conversation being read, empty on the list.
	open string

	conversations []client.LocalConversation
	entries       []client.LocalEntry
	reactions     map[int64]map[string]int

	draft   string
	query   string
	results []client.SearchResult

	status string
	width  int
	height int
}

func newModel(ctx context.Context, api *client.API, store *client.Store, syncer *client.Syncer) model {
	return model{
		ctx: ctx, api: api, store: store, syncer: syncer,
		view: viewConversations, width: 80, height: 24,
		status: "syncing…",
	}
}

func (m model) Init() tea.Cmd {
	return m.load()
}

// load reads the store. Always the store: a UI that reads the network has a UI that
// blocks on the network.
func (m model) load() tea.Cmd {
	ctx, store, open := m.ctx, m.store, m.open
	return func() tea.Msg {
		conversations, err := store.Conversations(ctx)
		if err != nil {
			return failed{err}
		}

		var (
			entries   []client.LocalEntry
			reactions map[int64]map[string]int
		)
		if open != "" {
			if entries, err = store.Entries(ctx, open); err != nil {
				return failed{err}
			}
			if reactions, err = store.ReactionCounts(ctx, open); err != nil {
				return failed{err}
			}
		}
		return loaded{conversations: conversations, entries: entries, reactions: reactions}
	}
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch typed := message.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = typed.Width, typed.Height
		return m, nil

	case storeChanged:
		return m, m.load()

	case loaded:
		m.conversations = typed.conversations
		m.entries = typed.entries
		m.reactions = typed.reactions
		if m.status == "syncing…" {
			m.status = "ready"
		}
		return m, nil

	case searched:
		m.results = typed.results
		return m, nil

	case syncFailed:
		// Shown rather than fatal. Offline is an ordinary state for a client with a
		// local store, and the interface keeps working from it.
		m.status = "offline: " + typed.err.Error()
		return m, nil

	case failed:
		m.status = "error: " + typed.err.Error()
		return m, nil

	case tea.KeyMsg:
		return m.key(typed)
	}
	return m, nil
}

func (m model) key(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch m.view {
	case viewConversations:
		return m.keyOnList(key)
	case viewConversation:
		return m.keyInConversation(key)
	case viewSearch:
		return m.keyInSearch(key)
	}
	return m, nil
}

func (m model) keyOnList(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c", "q":
		return m, tea.Quit

	case "up", "k":
		if m.selected > 0 {
			m.selected--
		}
		return m, nil

	case "down", "j":
		if m.selected < len(m.conversations)-1 {
			m.selected++
		}
		return m, nil

	case "enter":
		if m.selected < len(m.conversations) {
			m.open = m.conversations[m.selected].ID
			m.view = viewConversation
			return m, tea.Batch(m.load(), m.acknowledge())
		}
		return m, nil

	case "/":
		m.view = viewSearch
		m.query = ""
		m.results = nil
		return m, nil

	case "r":
		return m, m.refresh()
	}
	return m, nil
}

func (m model) keyInConversation(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c":
		return m, tea.Quit

	case "esc":
		m.view = viewConversations
		m.open = ""
		m.draft = ""
		return m, m.load()

	case "enter":
		text := strings.TrimSpace(m.draft)
		if text == "" {
			return m, nil
		}
		m.draft = ""
		return m, m.send(text)

	case "backspace":
		if m.draft != "" {
			m.draft = m.draft[:len(m.draft)-1]
		}
		return m, nil

	default:
		if runes := key.Runes; len(runes) > 0 {
			m.draft += string(runes)
		} else if key.String() == " " {
			m.draft += " "
		}
		return m, nil
	}
}

func (m model) keyInSearch(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c":
		return m, tea.Quit

	case "esc":
		m.view = viewConversations
		return m, nil

	case "backspace":
		if m.query != "" {
			m.query = m.query[:len(m.query)-1]
		}
		return m, m.search()

	default:
		if runes := key.Runes; len(runes) > 0 {
			m.query += string(runes)
		} else if key.String() == " " {
			m.query += " "
		}
		return m, m.search()
	}
}

// --- commands ---

func (m model) send(text string) tea.Cmd {
	ctx, syncer, conversationID := m.ctx, m.syncer, m.open
	// The identifier is generated once here and stored with the pending send, so a
	// retry after a failure is the same entry rather than a second one (MS-2).
	clientEntryID := id.New()

	return func() tea.Msg {
		if err := syncer.Send(ctx, conversationID, clientEntryID, text, 0); err != nil {
			// Not an error the person needs to act on: the send is pending and the next
			// connection flushes it. Reported as status so it is visible.
			return syncFailed{err}
		}
		return storeChanged{}
	}
}

func (m model) search() tea.Cmd {
	ctx, store, query := m.ctx, m.store, m.query
	return func() tea.Msg {
		results, err := store.Search(ctx, query, 50)
		if err != nil {
			return failed{err}
		}
		return searched{results}
	}
}

func (m model) refresh() tea.Cmd {
	ctx, syncer := m.ctx, m.syncer
	return func() tea.Msg {
		if err := syncer.Refresh(ctx); err != nil {
			return syncFailed{err}
		}
		return storeChanged{}
	}
}

// acknowledge reports what opening a conversation means: everything held is delivered
// and read.
func (m model) acknowledge() tea.Cmd {
	ctx, api, conversationID := m.ctx, m.api, m.open
	var highest int64
	for _, entry := range m.entries {
		highest = max(highest, entry.Sequence)
	}
	for _, conversation := range m.conversations {
		if conversation.ID == conversationID {
			highest = max(highest, conversation.Contiguous)
		}
	}
	if highest == 0 {
		return nil
	}

	return func() tea.Msg {
		if err := api.Acknowledge(ctx, conversationID, highest, highest); err != nil {
			return syncFailed{err}
		}
		return storeChanged{}
	}
}

// --- rendering ---

var (
	titleStyle    = lipgloss.NewStyle().Bold(true)
	mutedStyle    = lipgloss.NewStyle().Faint(true)
	selectedStyle = lipgloss.NewStyle().Reverse(true)
	badgeStyle    = lipgloss.NewStyle().Bold(true)
	mineStyle     = lipgloss.NewStyle().Bold(true)
)

func (m model) View() string {
	switch m.view {
	case viewConversation:
		return m.viewConversation()
	case viewSearch:
		return m.viewSearch()
	default:
		return m.viewList()
	}
}

func (m model) viewList() string {
	var out strings.Builder
	out.WriteString(titleStyle.Render("comms") + "  " + mutedStyle.Render(m.status) + "\n\n")

	if len(m.conversations) == 0 {
		out.WriteString(mutedStyle.Render("no conversations yet\n"))
	}

	for index, conversation := range m.conversations {
		label := conversation.Label
		if label == "" {
			label = conversation.Kind + " " + conversation.ID[:8]
		}

		line := fmt.Sprintf("  %-40s", label)
		if conversation.Unread > 0 {
			line += badgeStyle.Render(fmt.Sprintf(" %d", conversation.Unread))
		}
		// The mark is on screen because it is what sync turns on, and the fastest way
		// to see a gap opening and closing.
		line += mutedStyle.Render(fmt.Sprintf("  #%d/%d", conversation.Contiguous, conversation.Head))

		if index == m.selected {
			line = selectedStyle.Render(line)
		}
		out.WriteString(line + "\n")
	}

	out.WriteString("\n" + mutedStyle.Render("↑↓ move · enter open · / search · r refresh · q quit"))
	return out.String()
}

func (m model) viewConversation() string {
	var out strings.Builder
	out.WriteString(titleStyle.Render("conversation") + "  " + mutedStyle.Render(m.status) + "\n\n")

	me := m.api.AccountID()
	for _, entry := range m.entries {
		// Amendments are applied to their target's row when they are stored, so they
		// are not themselves messages on screen. Skipped rather than rendered.
		if entry.TargetSequence != 0 {
			continue
		}

		who := entry.AuthorID
		if len(who) > 8 {
			who = who[:8]
		}
		if entry.AuthorID == me {
			who = mineStyle.Render("you")
		}

		body := entry.Body
		if entry.Kind == "retracted" {
			body = mutedStyle.Render("(deleted)")
		}
		// A marker, not the photo. A terminal cannot show one and this does not pretend
		// to — but a caption-less photo would otherwise render as a blank line, which
		// reads as a bug rather than as a message this client cannot display.
		if entry.AttachmentID != "" && entry.Kind != "retracted" {
			marker := mutedStyle.Render("[attachment]")
			if body == "" {
				body = marker
			} else {
				body = marker + " " + body
			}
		}

		line := fmt.Sprintf("%s %-10s %s", mutedStyle.Render(fmt.Sprintf("#%-4d", entry.Sequence)), who, body)
		if entry.ReplyTo != 0 {
			line = mutedStyle.Render(fmt.Sprintf("      ↳ replying to #%d", entry.ReplyTo)) + "\n" + line
		}
		out.WriteString(line)

		if counts := m.reactions[entry.Sequence]; len(counts) > 0 {
			var parts []string
			for emoji, count := range counts {
				parts = append(parts, fmt.Sprintf("%s%d", emoji, count))
			}
			out.WriteString("  " + mutedStyle.Render(strings.Join(parts, " ")))
		}
		out.WriteString("\n")
	}

	out.WriteString("\n> " + m.draft + "▌\n")
	out.WriteString(mutedStyle.Render("enter send · esc back"))
	return out.String()
}

func (m model) viewSearch() string {
	var out strings.Builder
	out.WriteString(titleStyle.Render("search") + "  " + mutedStyle.Render("local, works offline") + "\n\n")
	out.WriteString("/ " + m.query + "▌\n\n")

	if m.query != "" && len(m.results) == 0 {
		out.WriteString(mutedStyle.Render("no matches\n"))
	}
	for _, result := range m.results {
		out.WriteString(fmt.Sprintf("%s %s  %s\n",
			mutedStyle.Render(result.ConversationID[:8]),
			mutedStyle.Render(fmt.Sprintf("#%d", result.Sequence)),
			result.Body))
	}

	out.WriteString("\n" + mutedStyle.Render("esc back"))
	return out.String()
}
