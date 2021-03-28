package adb

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/errors"

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

func (f *Fs) runCommand(cmd string, args ...string) (string, int, error) {
	args = append(args, ";", "echo", ":$?")
	ret, err := f.device.RunCommand(cmd, args...)
	if err != nil {
		return "", 0, err
	}

	codePos := strings.LastIndex(ret, ":")
	code, err := strconv.Atoi(ret[codePos+1 : len(ret)-1])
	if err != nil {
		return "", 0, err
	}

	if codePos > 0 {
		return ret[0 : codePos-1], code, nil
	}
	return "", code, nil
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

func (f *Fs) newObject(root string, name string, size int64, mode os.FileMode, modTime time.Time) *Object {
	dir := f.root + "/" + root
	return &Object{
		fs:      f,
		path:    &dir,
		name:    name,
		remote:  strings.Trim(root+"/"+name, "/"),
		size:    size,
		mode:    mode,
		modTime: modTime,
	}
}

func (f *Fs) tryStat(path string) (os.FileMode, int64, time.Time, error) {
	file := "/" + strings.Trim(f.root+"/"+path, "/")
	ret, code, err := f.runCommand("stat", "-L", "-c", "%f %s %Y", file)
	if err != nil {
		return 0, 0, time.Time{}, err
	}

	if code != 0 {
		if strings.Contains(ret, "No such file or directory") {
			return 0, 0, time.Time{}, err
		}
		return 0, 0, time.Time{}, errors.Errorf("stat unknown %d: %s", code, ret)
	}

	modeStrings := strings.Split(ret, " ")
	mode, _ := strconv.ParseInt(modeStrings[0], 16, 64)
	size, _ := strconv.ParseInt(modeStrings[1], 10, 64)
	modTime, _ := strconv.ParseInt(modeStrings[2], 10, 64)

	if (mode & 0040000) != 0 {
		mode |= int64(os.ModeDir)
	}

	fileMode := os.FileMode(mode)
	return fileMode, size, time.Unix(modTime, 0), nil
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
	dir = "/" + strings.Trim(f.root+"/"+dir, "/")
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

		if (entry.Mode & os.ModeSymlink) != 0 {
			// reslove symbolic links and get real stat
			newMode, newSize, newModTime, err := f.tryStat(root + "/" + entry.Name)
			if err == nil {
				entry.Mode = newMode
				entry.Size = int32(newSize)
				entry.ModifiedAt = newModTime
			}
		}

		if entry.Mode.IsDir() {
			allEntries = append(allEntries, fs.NewDir(strings.Trim(root+"/"+entry.Name, "/"), entry.ModifiedAt))
		} else {
			allEntries = append(allEntries, f.newObject(
				root,
				entry.Name,
				int64(entry.Size),
				entry.Mode,
				entry.ModifiedAt))
		}
	}

	if len(allEntries) == 0 {
		// try to fill non-readable directories
		if dir == "/data" {
			allEntries = append(allEntries, fs.NewDir(strings.Trim(root+"/local", "/"), time.Now()))
			allEntries = append(allEntries, fs.NewDir(strings.Trim(root+"/app", "/"), time.Now()))
		} else if dir == "/data/local" {
			allEntries = append(allEntries, fs.NewDir(strings.Trim(root+"/tmp", "/"), time.Now()))
		} else if dir == "/data/app" || dir == "/system/vendor/app" || dir == "/vendor/overlay" {
			ret, code, err := f.runCommand("pm", "list", "packages", "-f")
			if err == nil && code == 0 {
				dirPos := len(dir) + 9
				for _, pack := range strings.Split(ret, "\n") {
					// package:/data/app/.../...apk={package name}
					if strings.HasPrefix(pack, "package:"+dir) {
						dirEndPos := strings.LastIndex(pack, "/")
						if dirEndPos != -1 && dirEndPos > dirPos {
							allEntries = append(allEntries, fs.NewDir(strings.Trim(root+"/"+pack[dirPos:dirEndPos], "/"), time.Now()))
						}
					}
				}
			}
		} else if dir == "/storage/emulated" {
			allEntries = append(allEntries, fs.NewDir(strings.Trim(root+"/0", "/"), time.Now()))
		}
	}

	return allEntries, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fileMode, size, modTime, err := f.tryStat(remote)
	if err != nil {
		return nil, err
	}

	if fileMode.IsDir() {
		return nil, fs.ErrorNotAFile
	}

	filePos := strings.LastIndex(remote, "/")
	if filePos == -1 {
		filePos = 0
		remote = "/" + remote
	}

	return f.newObject(remote[0:filePos], remote[filePos+1:], size, fileMode, modTime), nil
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
	remote := src.Remote()
	filePos := strings.LastIndex(remote, "/")
	if filePos == -1 {
		filePos = 0
		remote = "/" + remote
	}

	o := f.newObject(remote[0:filePos], remote[filePos+1:], src.Size(), os.FileMode(0644), src.ModTime(ctx))
	err := o.Update(ctx, in, src, options...)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	ret, code, err := f.runCommand("mkdir", "-p", f.root+"/"+dir)
	if err != nil {
		return err
	}

	if code != 0 {
		if !strings.Contains(ret, "File exists") {
			return errors.Errorf("mkdir return %d, %s", code, ret)
		}
	}

	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	ret, code, err := f.runCommand("rmdir", f.root+"/"+dir)
	if err != nil {
		return err
	}

	if code != 0 {
		if strings.Contains(ret, "Directory not empty") {
			return fs.ErrorDirectoryNotEmpty
		}
		return errors.Errorf("rm return %d, %s", code, ret)
	}

	return nil
}

func (o *Object) fullpath() string {
	return *o.path + "/" + o.name
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
	_, _, err := o.fs.runCommand("touch", "-m", "-d", t.Format("2006-01-02 15:04:05 -0700"), o.fs.root+"/"+o.remote)
	if err != nil {
		return err
	}

	return nil
}

type adbFileReader struct {
	o *Object
	start int64
	limit int64
	buf []byte
	bufPos int
}

func (a *adbFileReader) Read(p []byte) (n int, err error) {
	if a.bufPos >= len(a.buf) {
		if a.limit <= 0 {
			return 0, io.EOF
		}
		blocksize := int64(10 * 1024 * 1024)
		firstBlock := a.start / blocksize
		lastBlock := (a.start + a.limit + blocksize - 1) / blocksize
		if lastBlock > firstBlock + 1 {
			lastBlock = firstBlock + 1
		}
		firstByte := a.start % blocksize

		result, err := a.o.fs.device.RunCommand(
			"dd",
			"if="+a.o.fullpath(),
			fmt.Sprintf("bs=%d", blocksize),
			fmt.Sprintf("skip=%d", firstBlock),
			fmt.Sprintf("count=%d", lastBlock-firstBlock),
			"2>",
			"/dev/null",
		)

		if err != nil {
			return 0, err
		}

		endByte := firstByte + a.limit
		if int(endByte) > len(result) {
			endByte = int64(len(result))
		}

		readBytes := endByte - firstByte
		a.start += readBytes
		a.limit -= readBytes
		buf := []byte(result)
		a.buf = buf[firstByte:endByte]
		a.bufPos = 0
	}

	readCount := len(a.buf) - a.bufPos
	if readCount > len(p) {
		readCount = len(p)
	}

	copy(p[0:readCount], a.buf[a.bufPos:(a.bufPos + readCount)])
	a.bufPos += readCount
	return readCount, nil
}

func (a *adbFileReader) Close() error {
	return nil
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
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

	if offset == 0 && limit == o.size {
		// same as adb pull
		return o.fs.device.OpenRead(o.fullpath())
	}

	return &adbFileReader{
		o:      o,
		start:  offset,
		limit:  limit,
	}, nil
}

// Update in to the object with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Upload should either
// return an error or update the object properly (rather than e.g. calling panic).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	out, err := o.fs.device.OpenWrite(o.fullpath(), os.FileMode(0644), src.ModTime(ctx))
	if err != nil {
		return err
	}

	var newSize int64

	if n := src.Size(); n == -1 {
		newSize, err = io.Copy(out, in)
	} else {
		newSize, err = io.CopyN(out, in, n)
	}

	out.Close()
	if err == nil {
		o.size = newSize
	}
	return err
}

// Removes this object
func (o *Object) Remove(ctx context.Context) error {
	ret, code, err := o.fs.runCommand("rm", o.fullpath())
	if err != nil {
		return err
	}

	if code != 0 {
		return errors.Errorf("rm return %d, %s", code, ret)
	}

	return nil
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	ret, code, err := f.runCommand("rm", "-rf", f.root+"/"+dir)
	if err != nil {
		return err
	}

	if code != 0 {
		return errors.Errorf("rm return %d, %s", code, ret)
	}

	return nil
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
	srcFs := src.(*Object).fs
	ret, code, err := f.runCommand("cp", srcFs.root+"/"+src.Remote(), f.root+"/"+remote)
	if err != nil {
		return nil, err
	}

	if code != 0 {
		return nil, errors.Errorf("cp return %d, %s", code, ret)
	}

	filePos := strings.LastIndex(remote, "/")
	if filePos == -1 {
		filePos = 0
		remote = "/" + remote
	}

	return f.newObject(remote[0:filePos], remote[filePos+1:], src.Size(), os.FileMode(0644), src.ModTime(ctx)), nil
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
	ret, code, err := f.runCommand("mv", f.root+"/"+src.Remote(), f.root+"/"+remote)

	if err != nil {
		return nil, err
	}

	if code != 0 {
		if strings.Contains(ret, "No such file or directory") {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, errors.Errorf("mv return %d, %s", code, ret)
	}

	filePos := strings.LastIndex(remote, "/")
	if filePos == -1 {
		filePos = 0
		remote = "/" + remote
	}

	return f.newObject(remote[0:filePos], remote[filePos+1:], src.Size(), os.FileMode(0644), src.ModTime(ctx)), nil
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
