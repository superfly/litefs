litefs
======

A FUSE filesystem for interfacing with liteserver.


## Usage

```sh
litefs /path/to/mnt
```

After stopping the program, you must use `umount -f` to unmount it before
mounting it again.

```sh
umount -f /path/to/mnt
```


## Deploy

```
fly launch --name litefs --region ord --no-deploy

fly volumes create --region ams --size 1 donotuse
fly volumes create --region dfw --size 1 donotuse
fly volumes create --region nrt --size 1 donotuse
fly volumes create --region ord --size 1 donotuse

fly scale count 4

fly deploy
```

