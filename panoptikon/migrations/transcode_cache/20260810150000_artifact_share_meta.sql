-- Share metadata: the two things a clipboard/share of an artifact needs that
-- the artifact row could not answer.
--
-- `sha256` is the hash of the *artifact's own* bytes. The key already carries
-- the source hash, which says nothing about the encoded output — a receiver
-- verifying the bytes it was handed needs the output's own digest.
--
-- `download_name` is the human name the download would have carried, computed
-- once at publish time from the request that produced the artifact. It is not
-- derivable afterwards: the key knows the source hash and the settings, never
-- the file's path or whether the request was trimmed.
--
-- Both are nullable because every row committed by an older build has neither,
-- and a re-encode is not worth forcing for a name. NULL is a normal state:
-- the share path falls back to the on-disk `<key>.<ext>` name and makes no
-- integrity claim, and the row heals whenever the artifact is re-committed.
ALTER TABLE artifacts ADD COLUMN sha256 TEXT;
ALTER TABLE artifacts ADD COLUMN download_name TEXT;
