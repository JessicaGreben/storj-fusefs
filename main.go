package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"storj.io/uplink"
)

func main() {
	access := flag.String("access", "", "access grant to a Storj project")
	bucketname := flag.String("bucket", "", "name of the Stroj bucket to be mounted")
	mountpoint := flag.String("mountpoint", "", "path to mount Storj bucket")
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

	err = fs.Serve(c, NewFS(project, *bucketname))
	if err != nil {
		log.Fatal(err)
	}
}

func setupUplink(ctx context.Context, access, bucketname string) *uplink.Project {
	a, err := uplink.ParseAccess(access)
	if err != nil {
		log.Fatal("parseAccess ", err)
		return nil
	}
	project, err := uplink.OpenProject(ctx, a)
	if err != nil {
		log.Fatal("OpenProject ", err)
		return nil
	}

	// check that the bucket exists
	_, err = project.StatBucket(ctx, bucketname)
	if err != nil {
		log.Fatal("StatBucket ", err)
		return nil
	}
	return project
}

func ls(ctx context.Context, p *uplink.Project, bucketname string) {
	iter := p.ListObjects(ctx, bucketname, nil)
	for iter.Next() {
		fmt.Println(iter.Item())
	}
	if err := iter.Err(); err != nil {
		log.Fatal("listObj ", err)
	}
}

type FS struct {
	root fs.Node
}

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

func NewDir(project *uplink.Project, bucketname string) *Dir {
	return &Dir{
		bucketname: bucketname,
		project:    project,
	}
}

func (Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0o555
	return nil
}

// A LookupRequest asks to look up the given name in the directory named by r.Node.
func (d *Dir) Lookup(ctx context.Context, objName string) (fs.Node, error) {
	if f, ok := d.containsObj(ctx, objName); ok {
		return f, nil
	}
	return nil, syscall.ENOENT
}

func (d *Dir) containsObj(ctx context.Context, objName string) (*File, bool) {
	iter := d.project.ListObjects(ctx, d.bucketname, nil)
	for iter.Next() {
		if iter.Item().Key == objName {
			return newFile(iter.Item(), d.project, d.bucketname), true
		}
	}
	if err := iter.Err(); err != nil {
		log.Fatal("listObj ", err)
		return nil, false
	}
	return nil, false
}

func (d Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var inodeInts uint64 = 2
	var dirDirs = []fuse.Dirent{}

	iter := d.project.ListObjects(ctx, d.bucketname, nil)
	for iter.Next() {
		entry := fuse.Dirent{
			Inode: inodeInts,
			Name:  iter.Item().Key,
			Type:  fuse.DT_File,
		}
		dirDirs = append(dirDirs, entry)
		inodeInts++
	}
	if err := iter.Err(); err != nil {
		log.Fatal("listObj ", err)
		return dirDirs, err
	}

	return dirDirs, nil
}

type File struct {
	obj        *uplink.Object
	project    *uplink.Project
	bucketname string
}

func newFile(obj *uplink.Object, project *uplink.Project, bucketname string) *File {
	return &File{
		obj:        obj,
		project:    project,
		bucketname: bucketname,
	}
}

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 2
	a.Mode = 0o444
	a.Size = uint64(f.obj.System.ContentLength)
	return nil
}

func (f File) ReadAll(ctx context.Context) ([]byte, error) {
	object, err := f.project.DownloadObject(ctx, f.bucketname, f.obj.Key, nil)
	if err != nil {
		log.Fatal("download: ", err)
	}
	defer object.Close()

	b, err := ioutil.ReadAll(object)
	if err != nil {
		log.Fatal("3: ", err)
	}
	return b, nil
}

func read(ctx context.Context, p *uplink.Project, bucketname string) {
	object, err := p.DownloadObject(ctx, bucketname, "go.mod", nil)
	if err != nil {
		log.Fatal("download: ", err)
	}
	defer object.Close()

	w, err := os.Create("here.sum")
	if err != nil {
		log.Fatal("2: ", err)
	}
	defer w.Close()
	_, err = io.Copy(w, object)
	if err != nil {
		log.Fatal("3: ", err)
	}
}
