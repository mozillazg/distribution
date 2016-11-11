package qiniu

import (
	"io"
	"sync"

	"qiniupkg.com/x/bytes.v7"
	"qiniupkg.com/x/rpc.v7"
	"qiniupkg.com/x/xlog.v7"

	"encoding/base64"
	"fmt"
	"hash/crc32"
	"net/http"
	"strconv"

	. "golang.org/x/net/context"
	"qiniupkg.com/api.v7/kodocli"
	"github.com/getsentry/raven-go"
)

const (
	defaultWorkers   = 4
	// defaultChunkSize = 256 * 1024 // 256k
	defaultTryTimes  = 3
)

// ----------------------------------------------------------

var tasks chan func()

func worker(tasks chan func()) {
	for {
		task := <-tasks
		task()
	}
}

func initWorkers() {

	tasks = make(chan func(), 3*defaultWorkers)
	for i := 0; i < defaultWorkers; i++ {
		go worker(tasks)
	}
}

// ----------------------------------------------------------

const (
	blockBits = 22
	blockMask = (1 << blockBits) - 1
)

func BlockCount(fsize int64) int {
	return int((fsize + blockMask) >> blockBits)
}

var once sync.Once

func rput(
	p kodocli.Uploader, ctx Context, ret interface{}, uptoken string,
	key string, hasKey bool, f io.ReaderAt, fsize int64, extra *kodocli.RputExtra) error {

	once.Do(initWorkers)

	log := xlog.NewWith(ctx)
	blockCnt := BlockCount(fsize)

	if extra == nil {
		extra = new(kodocli.RputExtra)
	}
	if extra.Progresses == nil {
		extra.Progresses = make([]kodocli.BlkputRet, blockCnt)
	} else if len(extra.Progresses) != blockCnt {
		return kodocli.ErrInvalidPutProgress
	}

	if extra.ChunkSize == 0 {
		extra.ChunkSize = defaultChunkSize
	}
	if extra.TryTimes == 0 {
		extra.TryTimes = defaultTryTimes
	}

	// var wg sync.WaitGroup
	// wg.Add(blockCnt)

	last := blockCnt - 1
	blkSize := 1 << blockBits
	nfails := 0
	p.Conn.Client = newUptokenClient(uptoken, p.Conn.Transport)

	for i := 0; i < blockCnt; i++ {
		blkIdx := i
		blkSize1 := blkSize
		if i == last {
			offbase := int64(blkIdx) << blockBits
			blkSize1 = int(fsize - offbase)
		}
		task := func() {
			// defer wg.Done()
			tryTimes := extra.TryTimes
		lzRetry:
			err := resumableBput(p, ctx, &extra.Progresses[blkIdx], f, blkIdx, blkSize1, extra)
			if err != nil {
				if tryTimes > 1 {
					tryTimes--
					log.Info("resumable.Put retrying ...")
					goto lzRetry
				}
				log.Warn("resumable.Put", blkIdx, "failed:", err)
				nfails++
			}
		}
		// tasks <- task
		task()
	}

	// wg.Wait()
	if nfails != 0 {
		return kodocli.ErrPutFailed
	}
	return nil
}

type uptokenTransport struct {
	token     string
	Transport http.RoundTripper
}

func (t *uptokenTransport) NestedObject() interface{} {
	return t.Transport
}

func (t *uptokenTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	defer func() {
		if err != nil {
		raven.CaptureError(err, nil)
		}
	}()

	req.Header.Set("Authorization", t.token)
	return t.Transport.RoundTrip(req)
}

func newUptokenTransport(token string, transport http.RoundTripper) *uptokenTransport {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &uptokenTransport{"UpToken " + token, transport}
}

func newUptokenClient(token string, transport http.RoundTripper) *http.Client {
	t := newUptokenTransport(token, transport)
	return &http.Client{Transport: t}
}

// ----------------------------------------------------------

func mkblk(
	p kodocli.Uploader, ctx Context, ret *kodocli.BlkputRet, blockSize int, body io.Reader, size int) error {

	url := p.UpHosts[0] + "/mkblk/" + strconv.Itoa(blockSize)
	return p.Conn.CallWith(ctx, ret, "POST", url, "application/octet-stream", body, size)
}

func bput(
	p kodocli.Uploader, ctx Context, ret *kodocli.BlkputRet, body io.Reader, size int) error {

	url := ret.Host + "/bput/" + ret.Ctx + "/" + strconv.FormatUint(uint64(ret.Offset), 10)
	return p.Conn.CallWith(ctx, ret, "POST", url, "application/octet-stream", body, size)
}

func resumableBput(
	p kodocli.Uploader,
	ctx Context, ret *kodocli.BlkputRet, f io.ReaderAt, blkIdx, blkSize int,
	extra *kodocli.RputExtra) (err error) {
	defer func() {
		if err != nil {
		raven.CaptureError(err, nil)
		}
	}()


	log := xlog.NewWith(ctx)
	h := crc32.NewIEEE()
	offbase := int64(blkIdx) << blockBits
	chunkSize := extra.ChunkSize

	var bodyLength int

	if ret.Ctx == "" {

		if chunkSize < blkSize {
			bodyLength = chunkSize
		} else {
			bodyLength = blkSize
		}

		body1 := io.NewSectionReader(f, offbase, int64(bodyLength))
		body := io.TeeReader(body1, h)

		err = mkblk(p, ctx, ret, blkSize, body, bodyLength)
		if err != nil {
			return
		}
		if ret.Crc32 != h.Sum32() || int(ret.Offset) != bodyLength {
			err = kodocli.ErrUnmatchedChecksum
			return
		}
		// extra.Notify(blkIdx, blkSize, ret)
	}

	for int(ret.Offset) < blkSize {

		if chunkSize < blkSize-int(ret.Offset) {
			bodyLength = chunkSize
		} else {
			bodyLength = blkSize - int(ret.Offset)
		}

		tryTimes := extra.TryTimes

	lzRetry:
		h.Reset()
		body1 := io.NewSectionReader(f, offbase+int64(ret.Offset), int64(bodyLength))
		body := io.TeeReader(body1, h)

		err = bput(p, ctx, ret, body, bodyLength)
		if err == nil {
			if ret.Crc32 == h.Sum32() {
				// extra.Notify(blkIdx, blkSize, ret)
				continue
			}
			log.Warn("ResumableBlockput: invalid checksum, retry")
			err = kodocli.ErrUnmatchedChecksum
		} else {
			if ei, ok := err.(*rpc.ErrorInfo); ok && ei.Code == kodocli.InvalidCtx {
				ret.Ctx = "" // reset
				log.Warn("ResumableBlockput: invalid ctx, please retry")
				return
			}
			log.Warn("ResumableBlockput: bput failed -", err)
		}
		if tryTimes > 1 {
			tryTimes--
			log.Info("ResumableBlockput retrying ...")
			goto lzRetry
		}
		break
	}
	return
}

func mkfile(
	p kodocli.Uploader, ctx Context, ret interface{}, key string, hasKey bool, fsize int64,
	extra *kodocli.RputExtra) (err error) {

	defer func() {
		if err != nil {
		raven.CaptureError(err, nil)
		}
	}()

	url := p.UpHosts[0] + "/mkfile/" + strconv.FormatInt(fsize, 10)

	if extra.MimeType != "" {
		url += "/mimeType/" + encode(extra.MimeType)
	}
	if hasKey {
		url += "/key/" + encode(key)
	}
	for k, v := range extra.Params {
		url += fmt.Sprintf("/%s/%s", k, encode(v))
	}

	buf := make([]byte, 0, 176*len(extra.Progresses))
	for _, prog := range extra.Progresses {
		buf = append(buf, prog.Ctx...)
		buf = append(buf, ',')
	}
	if len(buf) > 0 {
		buf = buf[:len(buf)-1]
	}

	return p.Conn.CallWith(
		ctx, ret, "POST", url, "application/octet-stream", bytes.NewReader(buf), len(buf))
}

// ----------------------------------------------------------

func encode(raw string) string {
	return base64.URLEncoding.EncodeToString([]byte(raw))
}
