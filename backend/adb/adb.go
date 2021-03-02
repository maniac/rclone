package adb

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"

	adb "github.com/zach-klippenstein/goadb"
)

// Fs represents a adb device
type Fs struct {
	name     string
	root     string
	s        string
	features *fs.Features // optional features
	device   *adb.Device
	service  *adb.Adb
}

type Object struct {
	fs      *Fs
	path    *string
	name    string
	remote  string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

type Options struct {
	Port   int    `config:"port"`
	Serial string `config:"serial"`
}

// Register with Fs
func init() {
	fsi := &fs.RegInfo{
		Name:        "adb",
		Description: "Adb interface",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "serial",
			Help:     "Device Serial Number",
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

	service, err := adb.NewWithConfig(adb.ServerConfig{
		Port: opt.Port,
	})

	if err != nil {
		return nil, err
	}

	desc := adb.AnyDevice()
	if opt.Serial != "" {
		desc = adb.DeviceWithSerial(opt.Serial)
	}

	device := service.Device(desc)

	f := &Fs{
		name:    name,
		root:    root,
		s:       "",
		device:  device,
		service: service,
	}

	features := (&fs.Features{
		CanHaveEmptyDirectories: true,
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
	root := dir
	dir = f.root + "/" + dir

	list, err := f.device.ListDirEntries(dir)
	if err != nil {
		return nil, err
	}

	allEntries := make(fs.DirEntries, 0, 64)
	for list.Next() {
		entry := list.Entry()
		if entry.Name == "." || entry.Name == ".." {
			continue
		}

		if entry.Mode.IsDir() {
			d := fs.NewDir(strings.Trim(root+"/"+entry.Name, "/"), entry.ModifiedAt)
			allEntries = append(allEntries, d)
		} else {
			obj := Object{
				fs:      f,
				path:    &dir,
				name:    entry.Name,
				remote:  strings.Trim(root+"/"+entry.Name, "/"),
				size:    int64(entry.Size),
				mode:    entry.Mode,
				modTime: entry.ModifiedAt,
			}
			allEntries = append(allEntries, &obj)
		}
	}

	return allEntries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return nil, fs.ErrorObjectNotFound
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
	return nil, fs.ErrorObjectNotFound
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	panic("not implemented") // TODO: Implement
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	panic("not implemented") // TODO: Implement
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
	return nil
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var offset, limit int64 = 0, o.size
	filename := *o.path + "/" + o.name

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

	if offset == 0 && limit == o.size {
		// same as adb pull
		return o.fs.device.OpenRead(filename)
	}

	blocksize := int64(65536)
	firstBlock := int64(offset / blocksize)
	lastBlock := int64((offset + limit + blocksize - 1) / blocksize)
	if firstBlock < 0 {
		firstBlock = 0
	}

	firstByte := offset - firstBlock*blocksize
	result, err := o.fs.device.RunCommand(
		"dd",
		"if="+filename,
		fmt.Sprintf("bs=%d", blocksize),
		fmt.Sprintf("skip=%d", firstBlock),
		fmt.Sprintf("count=%d", lastBlock-firstBlock),
	)

	if err == nil {
		return ioutil.NopCloser(strings.NewReader(result[firstByte:(firstByte + limit)])), nil
	}
	return nil, err
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
	// call rm -f {PATH}
	return fs.ErrorPermissionDenied
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	// call rm -rf {PATH}
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
	// call cp {SRC} {REMOTE}
	return nil, fs.ErrorPermissionDenied
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
	// call mv {SRC} {REMOTE}
	return nil, fs.ErrorPermissionDenied
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
	return fs.ErrorPermissionDenied
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	result, err := f.device.RunCommand("df", "-a", f.root)

	if err != nil {
		return nil, err
	}

	usage := &fs.Usage{}
	field := 0
	for _, v := range strings.Split(strings.Split(result, "\n")[1], " ") {
		if v != "" {
			switch field {
			case 1: // total
				val, _ := strconv.ParseInt(v, 10, 64)
				val *= 1024
				usage.Total = &val
			case 2: // used
				val, _ := strconv.ParseInt(v, 10, 64)
				val *= 1024
				usage.Used = &val
			case 3:
				val, _ := strconv.ParseInt(v, 10, 64)
				val *= 1024
				usage.Free = &val
				break
			}
			field++
		}
	}

	return usage, nil
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}
