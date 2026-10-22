package pulse

import "time"

// The primitive kinds.
//
// An agent executes primitives and knows nothing about what they are for:
// probing a certificate, running a playbook and updating a trust store are all
// the same few operations with different content. The knowledge — git,
// credentials, inventory, certificate policy — lives on the server, which
// compiles it into primitives. See docs/design.md §3.
//
// Naming is `family.action`. The family is the unit of authorisation ("this
// agent may only do net.*", see LocalPolicy); the action carries the
// idempotence promise.
const (
	// KindExecRun runs a program in a single-use workspace. The keystone: it
	// covers most of "do something on that host".
	KindExecRun = "exec.run"

	// KindFSPut writes one persistent file, atomically and idempotently.
	// Needs no subprocess and nothing installed on the host.
	KindFSPut = "fs.put"
	// KindFSGet reads a file, bounded in size.
	KindFSGet = "fs.get"
	// KindFSStat reports existence, mode, size, checksum.
	KindFSStat = "fs.stat"
	// KindFSRemove deletes a path.
	KindFSRemove = "fs.remove"

	// KindNetTLSProbe performs a TLS handshake and reports the certificates
	// seen. Reports observations, never verdicts — "20 days left" is a fact,
	// "20 days is a warning" is an application's policy.
	KindNetTLSProbe = "net.tlsprobe"

	// KindAgentInfo collects host facts.
	KindAgentInfo = "agent.info"
	// KindAgentUpgrade replaces the agent's own binary.
	KindAgentUpgrade = "agent.upgrade"
)

// Stream names for JobProgress.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// --- exec.run ---

// ExecRun runs a program in a single-use workspace.
//
// The workspace is created when the job starts and removed when it ends,
// whatever the outcome. A session model — open a workspace, run several things,
// close it — was considered and rejected: if the server dies mid-session the
// host keeps a directory holding an SSH private key, and the instruction to
// clean it up never arrives. Self-contained jobs let the agent clean up on its
// own, through crashes and restarts alike.
type ExecRun struct {
	// Files are materialised in the workspace, keyed by path relative to it:
	// a playbook tree, an inventory, an SSH key, a script.
	Files map[string]File `json:"files,omitempty"`
	// Blobs are large content fetched by digest before the program runs.
	Blobs map[string]Blob `json:"blobs,omitempty"`

	// Argv is the program and its arguments. Argv[0] is the executable, and
	// this is NOT passed through a shell.
	//
	// That is a protocol guarantee rather than an implementation detail: it
	// removes the shell injection surface, and it is what makes an executable
	// allowlist meaningful — permitting /bin/sh would permit everything. So
	// there is deliberately nowhere in this payload to put a shell fragment.
	// Work that needs a pipeline puts a script in Files and executes that,
	// where its content is auditable.
	Argv []string `json:"argv"`

	// Env holds environment variables, and is where secrets go.
	//
	// Not Argv: that is visible in `ps` to every user on the host. This is not
	// hypothetical — go-ansible's issue #140 shows a real command line
	// carrying `--extra-vars {"ansible_sudo_pass":...}`, readable by anyone
	// logged in. An environment lives in /proc/<pid>/environ, which needs the
	// same user or root. Not perfect, but an order of magnitude better.
	//
	// An agent must never log these values or include them in a result; only
	// the key names.
	Env map[string]string `json:"env,omitempty"`

	// Stdin is fed to the process: a sudo password, a vault password.
	Stdin string `json:"stdin,omitempty"`
	// Dir is the working directory relative to the workspace. Empty means its
	// root.
	Dir string `json:"dir,omitempty"`

	// Capture names files to read back when the program exits — an Ansible
	// JSON result, a generated report. Each is size-bounded; over the bound it
	// is truncated and flagged.
	Capture []string `json:"capture,omitempty"`
	// StreamOutput sends stdout and stderr back as the job runs (JobProgress).
	// Off, the result carries the last few KB instead: a command that finishes
	// in a second is not worth streaming.
	StreamOutput bool `json:"stream_output,omitempty"`
}

// File is content to materialise in a workspace.
type File struct {
	Content string `json:"content"`
	// Mode is an octal string ("0600"). Empty means 0644.
	Mode string `json:"mode,omitempty"`
}

// Blob is large content referenced by digest, fetched with FetchBlob.
type Blob struct {
	// Digest is "sha256:<hex>".
	Digest string `json:"digest"`
	// Extract unpacks the blob as an archive at this path rather than writing
	// it as a single file.
	Extract bool `json:"extract,omitempty"`
}

// ExecResult is what exec.run reports.
type ExecResult struct {
	ExitCode int `json:"exit_code"`
	// Stdout and Stderr are the trailing bytes, bounded. The full output went
	// through JobProgress when StreamOutput was set.
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
	// Captured maps each requested path to its content.
	Captured map[string]string `json:"captured,omitempty"`
	// Truncated says some output or captured file hit its size bound. Flagged
	// rather than silent: a log that is visibly cut is safe to read, one that
	// is quietly cut is not.
	Truncated bool          `json:"truncated,omitempty"`
	Duration  time.Duration `json:"duration_ns,omitempty"`
}

// --- fs.* ---

// FSPut writes one persistent file atomically and idempotently.
//
// The most common operation in managing a host is changing a file, and this
// does it with no subprocess and nothing installed on the target — a static
// binary suffices.
type FSPut struct {
	Path string `json:"path"`
	// Content and Blob are alternatives. A CA bundle runs to hundreds of
	// kilobytes, so it travels as a blob.
	Content string `json:"content,omitempty"`
	Blob    string `json:"blob,omitempty"`

	// Mode is octal. Empty keeps the existing mode, or 0644 for a new file.
	Mode  string `json:"mode,omitempty"`
	Owner string `json:"owner,omitempty"`
	Group string `json:"group,omitempty"`

	// Backup keeps the replaced content so a rollback has something to restore.
	Backup bool `json:"backup,omitempty"`

	// ConfirmWithin arms a dead man's switch: if the agent does not reach the
	// server successfully within this long after the write, it restores the
	// backup by itself. Zero disables it, which also means Backup is moot.
	//
	// This is the one safety mechanism only an agent can implement. Replacing
	// a CA trust store can cut the agent's own lifeline — if the new bundle
	// omits the CA that signed the server's certificate, the agent can never
	// reconnect, and a thousand hosts go dark at once. The server cannot
	// recover from that, because it has lost precisely the channel it would
	// need; all it sees is an agent going offline, indistinguishable from a
	// network fault. The agent can.
	ConfirmWithin time.Duration `json:"confirm_within_ns,omitempty"`
}

// FSPutResult is what fs.put reports.
type FSPutResult struct {
	// Changed is false when the content was already what was asked for and
	// nothing was written.
	//
	// This is both the idempotence contract and a useful report: across a
	// thousand hosts, the `changed=true` rows are exactly the ones whose state
	// had drifted.
	Changed   bool   `json:"changed"`
	OldSHA256 string `json:"old_sha256,omitempty"`
	NewSHA256 string `json:"new_sha256"`
	// BackupPath is where the old content went, for a server-driven rollback.
	BackupPath string `json:"backup_path,omitempty"`
}

// FSGet reads a file.
type FSGet struct {
	Path string `json:"path"`
	// MaxBytes bounds the read. Zero means the agent's own default — an
	// unbounded fs.get of a log file would be a way to flood the server.
	MaxBytes int64 `json:"max_bytes,omitempty"`
}

// FSGetResult is what fs.get reports.
type FSGetResult struct {
	Content   string `json:"content"`
	SHA256    string `json:"sha256"`
	Truncated bool   `json:"truncated,omitempty"`
}

// FSStat asks about a path without reading it.
type FSStat struct {
	Path string `json:"path"`
	// Checksum computes the SHA-256 as well, which costs a full read.
	Checksum bool `json:"checksum,omitempty"`
}

// FSStatResult is what fs.stat reports.
type FSStatResult struct {
	Exists  bool      `json:"exists"`
	Mode    string    `json:"mode,omitempty"`
	Size    int64     `json:"size,omitempty"`
	ModTime time.Time `json:"mod_time,omitzero"`
	SHA256  string    `json:"sha256,omitempty"`
	IsDir   bool      `json:"is_dir,omitempty"`
}

// FSRemove deletes a path.
type FSRemove struct {
	Path string `json:"path"`
	// Recursive is required to remove a non-empty directory: deleting a tree
	// should be something the caller said out loud.
	Recursive bool `json:"recursive,omitempty"`
}

// FSRemoveResult is what fs.remove reports.
type FSRemoveResult struct {
	// Changed is false when the path was already absent.
	Changed bool `json:"changed"`
}

// --- agent.* ---

// AgentInfo asks an agent about its host.
//
// Binaries and Mounts are queries rather than a fixed inventory. Enumerating
// every installed package takes seconds per host and is pointless to repeat
// across a thousand of them — and a complete software inventory is
// reconnaissance, worth more to an attacker than a handful of versions. So the
// server asks for what it needs to know: before dispatching Ansible work, it
// wants to know which agents have ansible-playbook.
type AgentInfo struct {
	// Binaries are executables to look for, by name or absolute path.
	//
	// Probing a version means running the program, so this is bounded by the
	// same executable allowlist as exec.run — otherwise agent.info would be a
	// way around it.
	Binaries []string `json:"binaries,omitempty"`
	// Mounts are paths whose free space to report: will the exec.run
	// workspace fit?
	Mounts []string `json:"mounts,omitempty"`
}

// AgentFacts is what an agent observes about its own host.
//
// Everything here is an objective fact readable from the machine. Anything
// requiring judgement — whether this host counts as production, whether it
// should be upgraded — is a label or a server-side decision, not a fact.
//
// Facts complement labels rather than replacing them: a label carries human
// intent the machine cannot know (which business owns this host), and a fact
// carries truth a human cannot maintain accurately across a thousand hosts.
type AgentFacts struct {
	Hostname string `json:"hostname"`
	// OS, Distro and Version: "linux", "rhel", "8.9".
	OS      string `json:"os"`
	Distro  string `json:"distro,omitempty"`
	Version string `json:"version,omitempty"`
	Arch    string `json:"arch"`
	Kernel  string `json:"kernel,omitempty"`

	// Addresses are the host's own addresses, for "which subnet is this on".
	Addresses []string `json:"addresses,omitempty"`
	// PackageManager is "apk", "apt", "yum", "dnf"; empty when unrecognised.
	PackageManager string `json:"package_manager,omitempty"`

	Binaries map[string]BinaryFact `json:"binaries,omitempty"`
	Disk     map[string]DiskFact   `json:"disk,omitempty"`

	CollectedAt time.Time `json:"collected_at"`
}

// BinaryFact is one executable's presence and version.
type BinaryFact struct {
	// Path is empty when the executable was not found.
	Path    string `json:"path,omitempty"`
	Version string `json:"version,omitempty"`
}

// DiskFact is one mount point's space.
type DiskFact struct {
	TotalBytes uint64 `json:"total_bytes"`
	FreeBytes  uint64 `json:"free_bytes"`
}

// AgentUpgrade replaces the agent's own binary.
//
// The only primitive that touches the control channel itself, so it is the
// only one where the agent must be able to rescue itself. See
// docs/issues/0024.
type AgentUpgrade struct {
	// Version is what the new binary should report. The agent refuses a
	// major-version jump rather than guessing whether the protocol and config
	// format still match — better a loud error than a silent gamble.
	Version string `json:"version"`
	// Blob is the binary, by digest.
	Blob string `json:"blob"`
	// Signature is over the binary, verified with a public key from the
	// agent's LOCAL config.
	//
	// Signed with a key unrelated to the server's credential key. Using the
	// same master key would mean compromising the server also grants the
	// ability to sign, which makes the signature decorative. The key being
	// local means rotating it requires touching every host — inconvenient,
	// and that inconvenience is the point.
	Signature string `json:"signature"`
}

// AgentUpgradeResult is what agent.upgrade reports before it restarts.
type AgentUpgradeResult struct {
	FromVersion string `json:"from_version"`
	ToVersion   string `json:"to_version"`
	// Restarting says the swap is done and the process is about to exit. The
	// confirmation that it worked is the next heartbeat, from the new binary.
	Restarting bool `json:"restarting"`
}
