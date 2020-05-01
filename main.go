package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"storj.io/uplink"
)

func main() {
	access := flag.String("access", "", "access grant to a Storj project")
	bucketname := flag.String("bucket", "", "name of the Storj bucket to be mounted")
	mountpoint := flag.String("mountpoint", "", "location to mount the Storj bucket")
	flag.Parse()

	c, err := fuse.Mount(
		*mountpoint,
		fuse.FSName("storj"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	project := setupUplink(ctx, *access, *bucketname)
	defer project.Close()

	pwd, err := os.Getwd()
	if err != nil {
		log.Println("getwd err:", err)
	}
	path := filepath.Join(pwd, *mountpoint)
	log.Println(fmt.Sprintf("starting fuse... mounting bucket sj://%s at path %s", *bucketname, path))
	err = fs.Serve(c, NewFS(project, *bucketname))
	if err != nil {
		log.Fatal(err)
	}
}

type FS struct {
	root fs.Node
}

var _ fs.FS = (*FS)(nil)

func NewFS(project *uplink.Project, bucketname string) *FS {
	return &FS{
		root: NewDir(project, bucketname),
	}
}

func (fs FS) Root() (fs.Node, error) {
	return fs.root, nil
}

type Dir struct {
	bucketname string
	project    *uplink.Project
}

// fs.Node is the interface required of a file or directory.
var _ fs.Node = (*Dir)(nil)

var _ fs.HandleReadDirAller = (*Dir)(nil)

func NewDir(project *uplink.Project, bucketname string) *Dir {
	return &Dir{
		bucketname: bucketname,
		project:    project,
	}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o555
	return nil
}

// A LookupRequest asks to look up the given name in the directory named by r.Node.
func (d *Dir) Lookup(ctx context.Context, objName string) (fs.Node, error) {
	if f, ok := d.containsObject(ctx, objName); ok {
		return f, nil
	}
	return nil, syscall.ENOENT
}

func (d *Dir) containsObject(ctx context.Context, objName string) (*File, bool) {
	iter := d.project.ListObjects(ctx, d.bucketname, nil)
	for iter.Next() {
		if iter.Item().Key == objName {
			f := newFile(iter.Item(), d.project, d.bucketname)
			return f, true
		}
	}
	if err := iter.Err(); err != nil {
		log.Fatal("listObj ", err)
		return nil, false
	}
	return nil, false
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// A Dirent represents a single directory entry.
	var dirDirs = []fuse.Dirent{}

	iter := d.project.ListObjects(ctx, d.bucketname, nil)
	for iter.Next() {
		entry := fuse.Dirent{
			Name: iter.Item().Key,
			Type: fuse.DT_File,
		}
		dirDirs = append(dirDirs, entry)
	}
	if err := iter.Err(); err != nil {
		log.Fatal("listObj: ", err)
		return dirDirs, err
	}

	return dirDirs, nil
}

type File struct {
	obj        *uplink.Object
	project    *uplink.Project
	bucketname string
}

// fs.Node is the interface required of a file or directory.
var _ fs.Node = (*File)(nil)

var _ fs.HandleReadAller = (*File)(nil)

func newFile(obj *uplink.Object, project *uplink.Project, bucketname string) *File {
	return &File{
		obj:        obj,
		project:    project,
		bucketname: bucketname,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	// todo: set valid to how long attr can be cached
	// a.Valid = time.Minute
	user, err := user.Current()
	if err != nil {
		log.Fatal("current user: ", err)
	}
	uid, err := strconv.Atoi(user.Uid)
	if err != nil {
		log.Fatal("atoi uid: ", err)
	}
	a.Uid = uint32(uid)
	// todo: dont hardcore
	a.Gid = uint32(1001)

	a.Mode = 0o444 // read only

	s, err := f.project.StatObject(ctx, f.bucketname, f.obj.Key)
	if err != nil {
		log.Fatal("object stat: ", err)
	}
	a.Size = uint64(s.System.ContentLength)
	return nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	object, err := f.project.DownloadObject(ctx, f.bucketname, f.obj.Key, nil)
	if err != nil {
		log.Fatal("download: ", err)
	}
	defer object.Close()

	b, err := ioutil.ReadAll(object)
	if err != nil {
		log.Fatal("readAll: ", err)
	}
	return b, nil
}
