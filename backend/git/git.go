package git

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	memory "github.com/go-git/go-git/v5/storage/memory"
)

var _ = (*git.Line)(nil)
var _ = (*memory.IndexStorage)(nil)
var _ = (*plumbing.Hash)(nil)
var _ = (*object.Object)(nil)

// Fs represents a git commit
type Fs struct {
	name     string
	root     string
	s        string
	features *fs.Features // optional features

	repository *git.Repository
	hash       *plumbing.Hash
	commit     *object.Commit
}

type Object struct {
	fs      *Fs
	path    *string
	name    string
	remote  string
	size    int64
	mode    os.FileMode
	modTime time.Time
	content *object.File
}

type Options struct {
	RemoteURL  string `config:"remote"`
	Revision   string `config:"revision"`
	CommitTime bool   `config:"commit_time"`
}

// Register with Fs
func init() {
	fsi := &fs.RegInfo{
		Name:        "git",
		Description: "GIT interface",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "remote",
			Help:     "Remote repo",
			Required: true,
		}, {
			Name:     "revision",
			Help:     "revision string",
			Required: false,
			Default:  string(plumbing.HEAD),
		}, {
			Name:     "commit_time",
			Help:     "Make modtime to last commit time",
			Default:  false,
			Required: false,
		}},
	}
	fs.Register(fsi)
}

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	var repo *git.Repository
	if _, err = os.Stat(opt.RemoteURL); err == nil {
		repo, err = git.PlainOpen(opt.RemoteURL)
	} else {
		repo, err = git.CloneContext(ctx, memory.NewStorage(), nil, &git.CloneOptions{
			URL:        opt.RemoteURL,
			NoCheckout: true,
		})
	}

	if err != nil {
		return nil, err
	}

	rev, err := repo.ResolveRevision(plumbing.Revision(opt.Revision))
	if err != nil {
		return nil, err
	}

	commit, err := repo.CommitObject(*rev)
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name:       name,
		root:       root,
		s:          "",
		repository: repo,
		hash:       rev,
		commit:     commit,
	}

	features := (&fs.Features{
		CanHaveEmptyDirectories: false,
		SlowModTime:             opt.CommitTime,
	}).Fill(ctx, f)

	f.features = features
	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return f.s
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Returns the supported hash types of the filesystem
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively that doing a directory traversal.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	tree, err := f.commit.Tree()
	if err != nil {
		return err
	}

	seekDir := strings.Trim(f.root+"/"+dir, "/")

	if seekDir != "" {
		tree, err = tree.Tree(seekDir)
		if err != nil {
			return err
		}
	}

	dedupDir := map[string]struct{}{}
	modTime := f.commit.Author.When
	total := 0

	tree.Files().ForEach(func(file *object.File) error {
		total++
		dirList := strings.Split(file.Name, "/")
		dirCat := ""
		for i := 1; i < len(dirList); i++ {
			dirCat = dirList[i-1] + "/"
			testDir := dirCat[0 : len(dirCat)-1]
			if _, ok := dedupDir[testDir]; !ok {
				dedupDir[testDir] = struct{}{}
				callback(fs.DirEntries{fs.NewDir(strings.Trim(seekDir+"/"+testDir, "/"), modTime)})
			}
		}

		callback(fs.DirEntries{&Object{
			fs:      f,
			path:    &dir,
			name:    file.Name,
			remote:  strings.Trim(seekDir+"/"+file.Name, "/"),
			size:    file.Size,
			mode:    os.FileMode(file.Mode),
			modTime: modTime,
			content: file,
		}})

		return nil
	})

	if total == 0 {
		return fs.ErrorDirNotFound
	}

	return nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	tree, err := f.commit.Tree()
	if err != nil {
		return nil, err
	}

	seekDir := strings.Trim(f.root+"/"+dir, "/")

	if seekDir != "" {
		tree, err = tree.Tree(seekDir)
		if err != nil {
			return nil, err
		}
	}

	dedupDir := map[string]struct{}{}
	var allEntries fs.DirEntries
	modTime := f.commit.Author.When

	tree.Files().ForEach(func(file *object.File) error {
		if pos := strings.Index(file.Name, "/"); pos != -1 {
			inDir := file.Name[0:pos]
			if _, ok := dedupDir[inDir]; !ok {
				dedupDir[inDir] = struct{}{}
				allEntries = append(allEntries, fs.NewDir(strings.Trim(seekDir+"/"+inDir, "/"), modTime))
			}
		} else {
			allEntries = append(allEntries, &Object{
				fs:      f,
				path:    &dir,
				name:    file.Name,
				remote:  strings.Trim(seekDir+"/"+file.Name, "/"),
				size:    file.Size,
				mode:    os.FileMode(file.Mode),
				modTime: modTime,
				content: file,
			})
		}
		return nil
	})

	if len(allEntries) == 0 {
		return nil, fs.ErrorDirNotFound
	}

	return allEntries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	dir := strings.Trim(f.root+"/"+remote, "/")

	file, err := f.commit.File(dir)
	if err != nil {
		return nil, err
	}

	filePos := strings.LastIndex(dir, "/")
	var fileName string
	if filePos == -1 {
		fileName = dir
		dir = ""
	} else {
		fileName = dir[(filePos + 1):]
		dir = dir[0:filePos]
	}

	return &Object{
		fs:      f,
		path:    &dir,
		name:    fileName,
		remote:  dir,
		size:    file.Size,
		mode:    os.FileMode(file.Mode),
		modTime: f.commit.Author.When,
		content: file,
	}, nil
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, fs.ErrorPermissionDenied
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return fs.ErrorPermissionDenied
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return fs.ErrorPermissionDenied
}

// String returns a description of the Object
func (o *Object) String() string {
	return o.name
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification date of the file
// It should return a best guess if one isn't available
func (o *Object) ModTime(_ context.Context) time.Time {
	if o.fs.features.SlowModTime {
		originalPath := strings.Trim(o.fs.root+"/"+o.name, "/")
		ci, err := o.fs.repository.Log(&git.LogOptions{
			From:     o.fs.commit.Hash,
			FileName: &originalPath,
		})
		if err == nil {
			defer ci.Close()
			c, err := ci.Next()
			if err == nil {
				return c.Author.When
			}
		}
	}

	return o.modTime
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.size
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", nil
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return false
}

// SetModTime sets the metadata on the object to set the modification date
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorPermissionDenied
}

type readCloser struct {
	io.Reader
}

func (o *readCloser) Read(p []byte) (int, error) {
	return o.Reader.Read(p)
}

func (o *readCloser) Close() error {
	r := o.Reader
	if l, ok := r.(*io.LimitedReader); ok {
		// get original reader to call Close()
		r = l.R
	}

	if c, ok := r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	reader, err := o.content.Reader()
	if err != nil {
		return nil, err
	}

	var offset, limit int64 = 0, o.size

	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		case *fs.RangeOption:
			offset, limit = x.Decode(o.Size())
		}
	}

	if limit == -1 || offset+limit > o.size {
		limit = o.size - offset
	}

	if offset > 0 {
		_, err = io.CopyN(ioutil.Discard, reader, offset)
		if err != nil {
			reader.Close()
			return nil, err
		}
	}

	return &readCloser{io.LimitReader(reader, limit)}, nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return fs.ErrorPermissionDenied
}

// Removes this object
func (o *Object) Remove(ctx context.Context) error {
	return fs.ErrorPermissionDenied
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return fs.ErrorPermissionDenied
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorCantCopy
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorCantMove
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote string, dstRemote string) error {
	// call mv {SRC} {REMOTE}
	return fs.ErrorCantDirMove
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	return nil, fs.ErrorPermissionDenied
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}
