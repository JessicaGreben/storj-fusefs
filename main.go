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
	"time"

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
	start := time.Now()
	a.Mode = os.ModeDir | 0o555
	a.Valid = 5 * time.Minute
	a.Uid = getUserID()
	a.Gid = getGroupID()
	log.Println(time.Since(start).Milliseconds(), " (ms), dir Attr")
	return nil
}

// A LookupRequest asks to look up the given name in the directory named by r.Node.
func (d *Dir) Lookup(ctx context.Context, objName string) (fs.Node, error) {
	start := time.Now()
	if f, ok := d.containsObject(ctx, objName); ok {
		return f, nil
	}
	log.Println(time.Since(start).Milliseconds(), " (ms), dir lookup for object", objName)
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
	start := time.Now()
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

	log.Println(time.Since(start).Milliseconds(), "(ms), dir ReadDirAll")
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
	start := time.Now()
	a.Valid = 5 * time.Minute
	a.Mode = 0o444 // read only
	a.Uid = getUserID()
	a.Gid = getGroupID()

	s, err := f.project.StatObject(ctx, f.bucketname, f.obj.Key)
	if err != nil {
		log.Fatal("object stat: ", err)
	}
	a.Size = uint64(s.System.ContentLength)
	log.Println(time.Since(start).Milliseconds(), "(ms), file Attr for", f.obj.Key)
	return nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	start := time.Now()
	object, err := f.project.DownloadObject(ctx, f.bucketname, f.obj.Key, nil)
	if err != nil {
		log.Fatal("download: ", err)
	}
	defer object.Close()

	b, err := ioutil.ReadAll(object)
	if err != nil {
		log.Fatal("readAll: ", err)
	}
	log.Println(time.Since(start).Milliseconds(), "(ms), file ReadAll for", f.obj.Key)
	return b, nil
}

func getUserID() uint32 {
	// set user and group to the current user/group
	// otherwise defaults to root
	user, err := user.Current()
	if err != nil {
		log.Fatal("current user: ", err)
	}
	uid, err := strconv.Atoi(user.Uid)
	if err != nil {
		log.Fatal("atoi uid: ", err)
	}
	return uint32(uid)
}
func getGroupID() uint32 {
	// todo: dont hardcore
	return uint32(1001)
}
