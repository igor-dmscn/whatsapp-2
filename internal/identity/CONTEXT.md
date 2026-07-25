# Identity

Who an account is, how it proves that, and which devices act on its behalf. Sole owner of handles, credentials and devices — every other context references accounts by ID only.

## Language

**Account**:
The authenticated principal. Owns credentials, and is what messages are sent from and addressed to.
_Avoid_: user, profile, identity

**Handle**:
The unique, human-chosen public name by which an account is found and addressed.
_Avoid_: username, nickname, tag

**Credential**:
One means of proving control of an account. An account may hold several of different kinds — a password today, a passkey later.
_Avoid_: password, auth method, login

**Device**:
One client installation connected on behalf of an account. Devices acknowledge delivery, but they hold no read state — that belongs to a membership.
_Avoid_: client, connection, installation

**Session**:
A device's current authenticated state, holding the access and refresh tokens valid for it. Rotating a session replaces both at once, so the previous pair stops working the instant a new one is issued.
_Avoid_: token pair, login, auth state

**Passphrase**:
A secret long enough to be accepted as the basis of a password credential. Length is the only rule; composition requirements produce weaker secrets than length alone.
_Avoid_: password, secret, PIN
