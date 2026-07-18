-- Board-level editing-behavior flags (auto-layout, auto-crop, …), stored as
-- an opaque JSON object owned by the UI — the gateway never parses it, same
-- contract as pinboard_versions.layout. Flags live on the BOARD, not on
-- versions: a pinboard is defined by its layout, while these flags only
-- shape how future edits behave, so they must not create versions or make a
-- board "unsaved". NULL means the board was last saved by a client that
-- didn't send flags; the UI treats that as its frozen codec defaults.
ALTER TABLE pinboards ADD COLUMN flags TEXT;
