package domain

// This file holds the context's one domain service: a rule that is part of the
// model and belongs to no single aggregate.
//
// A function, not a type with one method. ADR-0010 says a domain service wrapping a
// single operation should not be a struct, and Go's package-level functions are
// already namespaced by the package — `domain.AuthoriseMembershipChange` reads no
// worse than a method and needs no construction or injection.

// AuthoriseMembershipChange reports whether an actor may change who belongs to a
// conversation.
//
// The reason this is not on either aggregate: the answer depends on the
// conversation's kind *and* the actor's role, and Conversation and Membership are
// separate roots that cannot see each other (see membership.go for why they are
// split). Putting it on Conversation would mean passing a role in and trusting the
// caller to have got it from the right membership; putting it on Membership would
// mean passing a kind in and doing the same. Both are the rule written down twice,
// with two chances to disagree.
//
// Direct conversations are refused outright rather than by role. The pair *is* the
// conversation's identity — that is what the unique key on it means — so there is no
// role that could be senior enough to add a third person without turning a private
// exchange into something two people did not agree to (MS-4).
func AuthoriseMembershipChange(conversation *Conversation, actor *Membership) error {
	if conversation == nil || actor == nil {
		return ErrNotAMember
	}
	if actor.ConversationID() != conversation.ID() {
		// A membership of some other conversation authorises nothing here.
		return ErrNotAMember
	}
	if conversation.Kind() == KindDirect {
		return ErrMembershipIsFixed
	}
	if !actor.MayAdminister() {
		return ErrNotPermittedToAdminister
	}
	return nil
}

// AuthoriseInvite reports whether an actor may create an invite to a conversation.
//
// The same rule: an invite is a deferred membership change, and it would be strange
// for the deferred form to be available to someone who cannot make the immediate
// one.
func AuthoriseInvite(conversation *Conversation, actor *Membership) error {
	return AuthoriseMembershipChange(conversation, actor)
}
