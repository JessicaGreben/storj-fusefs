# test plan

Test plan for testing a fuse filesystems

##### Read tests

- recursive diffs and make sure everything looks correct

```
diff -rq
```

- test offset reads, seeking around

Maybe the best way to test is to create a small program to open a file and seek around

- test opening large files

- test subdirectories

- test concurrent reads, opening 2 files at the same time

Make sure test programs are actually calling the fuse read methods and not buffering, might be hard if the kernel is doing it. Might need to indicate not to when open file.

- test metadata of files: mode, timestamps, block size and filesize. Make sure ls returns the right size and also du returns the right size of the file (du measures how many 512 bytes blocks in attr call, while ls uses the size in attr).

todo: is there fuse testing software for read only fuse

##### Write tests

Currently the fuse fs is read only, but down the road it will support writes.

- symlinks
- directories
- special character devices
