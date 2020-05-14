package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"storj.io/uplink"
)

var (
	uid = uint32(os.Geteuid())
	gid = uint32(os.Getegid())
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
		fmt.Println("getwd err:", err)
	}
	path := filepath.Join(pwd, *mountpoint)
	log.Println(fmt.Sprintf("starting fuse... mounting bucket sj://%s at path %s", *bucketname, path))

	err = fs.Serve(c, NewFS(project, *bucketname))
	if err != nil {
		log.Fatal(err)
	}
}

func logE(msg string, err error) error {
	if err != nil {
		log.Printf("%s error: %+v\n", msg, err)
	}
	return err
}

type FS struct {
	root fs.Node
}

var _ fs.FS = (*FS)(nil)

func NewFS(project *uplink.Project, bucketname string) *FS {
	return &FS{
		root: NewDir(project, bucketname, ""),
	}
}

func (fs FS) Root() (fs.Node, error) {
	return fs.root, nil
}

type Dir struct {
	bucketname string
	prefix     string
	project    *uplink.Project
}

var _ fs.Node = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)

func NewDir(project *uplink.Project, bucketname, prefix string) *Dir {
	return &Dir{
		bucketname: bucketname,
		prefix:     prefix,
		project:    project,
	}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Println("dir.Attr called")
	a.Mode = os.ModeDir | 0o555
	a.Uid = uid
	a.Gid = gid
	return nil
}

func (d *Dir) Lookup(ctx context.Context, objKey string) (fs.Node, error) {
	fmt.Println("dir.Lookup called")
	start := time.Now()

	objKey = d.prefix + objKey
	object, err := d.project.StatObject(ctx, d.bucketname, objKey)
	if err != nil {
		if errors.Is(err, uplink.ErrObjectNotFound) {
			// todo: currently this will create a new dir if the object
			// isn't found
			// ideally we would also want to listObjects to see if its a dir
			// with this name, and then if not return...
			// return nil, syscall.ENOENT
			// however we are deciding to do this for performance
			// Separate question: how do you handle mkdir (since storj doesnt
			// create a dir unless there is a file in it. S3 fuse handles this
			// by making special files with metadata about that this is a dir)
			d := NewDir(d.project, d.bucketname, objKey+"/")
			fmt.Println(time.Since(start).Milliseconds(),
				" ms, prefix dir lookup for object:", objKey,
			)
			return d, nil
		}
		return nil, logE("lookup StatObject", err)
	}

	f := newFile(object, d.project, d.bucketname)
	fmt.Println(time.Since(start).Milliseconds(),
		" ms, file dir lookup for object", object,
	)
	return f, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fmt.Println("dir.ReadDirAll called")
	start := time.Now()
	var dirDirs = []fuse.Dirent{}

	iter := d.project.ListObjects(ctx, d.bucketname, &uplink.ListObjectsOptions{Prefix: d.prefix})
	for iter.Next() {
		key := strings.TrimPrefix(iter.Item().Key, d.prefix)
		key = strings.TrimSuffix(key, "/")
		fmt.Println("list:", iter.Item().Key, key)
		entry := fuse.Dirent{
			Name: key,
			Type: fuse.DT_File,
		}
		if iter.Item().IsPrefix {
			fmt.Println("is prfix")
			entry.Type = fuse.DT_Dir
		}
		dirDirs = append(dirDirs, entry)
	}
	if err := iter.Err(); err != nil {
		return dirDirs, logE("iter.Err", err)
	}

	fmt.Println(time.Since(start).Milliseconds(), "ms, dir ReadDirAll")
	fmt.Println(dirDirs)
	return dirDirs, nil
}

type File struct {
	obj        *uplink.Object
	project    *uplink.Project
	bucketname string
}

var _ fs.Node = (*File)(nil)
var _ fs.HandleReader = (*File)(nil)

// var _ fs.HandleReadAller = (*File)(nil)

func newFile(obj *uplink.Object, project *uplink.Project, bucketname string) *File {
	return &File{
		obj:        obj,
		project:    project,
		bucketname: bucketname,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Println("file.Attr called")
	start := time.Now()
	a.Mode = 0o444
	a.Uid = uid
	a.Gid = gid

	s, err := f.project.StatObject(ctx, f.bucketname, f.obj.Key)
	if err != nil {
		return logE("file.Attr statObject", err)
	}
	a.Size = uint64(s.System.ContentLength)
	fmt.Println(time.Since(start).Milliseconds(), "ms, file Attr for", f.obj.Key)
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fmt.Println("file.Read called", f.obj.Key)
	fmt.Printf("req: #%v\n", req)
	d, err := f.project.DownloadObject(ctx,
		f.bucketname,
		f.obj.Key,
		&uplink.DownloadOptions{Offset: req.Offset, Length: int64(req.Size)},
	)
	fmt.Println("down 1")
	if err != nil {
		fmt.Println("down 2")
		return logE("Read DownloadObject", err)
	}

	defer d.Close()

	fmt.Println("req.Size:", req.Size)
	buf := make([]byte, req.Size)
	n, err := d.Read(buf)
	fmt.Println("n:", n)
	if err != nil {
		if err == io.EOF {
			resp.Data = buf[:n]
			return nil
		}
		return logE("Read", err)
	}
	fmt.Println("n:", n)
	resp.Data = buf[:]

	return nil
}

// func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
// 	fmt.Println("file.ReadAll called")
// 	start := time.Now()
// 	object, err := f.project.DownloadObject(ctx, f.bucketname, f.obj.Key, nil)
// 	if err != nil {
// 		return nil, logE("readAll download ", err)
// 	}
// 	defer object.Close()

// 	b, err := ioutil.ReadAll(object)
// 	if err != nil {
// 		return nil, logE("readAll", err)
// 	}
// 	fmt.Println(time.Since(start).Milliseconds(), "ms, file ReadAll for", f.obj.Key)
// 	return b, nil
// }
