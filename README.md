# Streaming large file uploads — Part 1 companion project

Companion code for the article **"Streaming Large File Uploads in Java Without Killing Your Server —
Part 1: True Streaming with Spring Boot WebFlux"**.

It is one Spring Boot WebFlux application that accepts an upload and writes it to disk *as the bytes
arrive*, plus a set of deliberately-broken endpoints that reproduce the ways this normally goes wrong.
Every claim in the article is checked by a test in this repository.

```
./mvnw verify                        # build + 15 fast tests
./mvnw verify -DskipITs=false        # + the proof: 512MB through a 256MB heap
```

---

## The finding that drives the article

Most WebFlux upload tutorials are subtly wrong in the same way. Spring's own reference says it plainly:

> To parse multipart data in streaming fashion, you can use the `Flux<PartEvent>` returned from the
> `PartEventHttpMessageReader` **instead of using `@RequestPart`, as that implies `Map`-like access to
> individual parts by name and, hence, requires parsing multipart data in full.**

So `@RequestPart` — in *any* form — cannot stream:

| Signature | What actually happens |
|---|---|
| `@RequestPart("file") MultipartFile` | Servlet stack: whole body buffered before your handler runs |
| `@RequestPart("file") FilePart` | WebFlux: whole part buffered to memory, then to a temp file (`.multipart`) before your handler runs |
| `@RequestPart("file") Flux<DataBuffer>` | WebFlux: joined into a **single** buffer capped by `max-in-memory-size` → **413** for anything over 256KB |
| `@RequestBody Flux<PartEvent>` | **Actually streams.** Events arrive as the body is parsed |

Boot's own configuration metadata agrees with the third row: `spring.webflux.multipart.file-storage-directory`
is documented as *"Directory used to store file parts larger than `maxInMemorySize`... **Ignored when using
the PartEvent streaming support**"*, and the old `spring.webflux.multipart.streaming` flag was deprecated
with the reason *"Replaced by the PartEventHttpMessageReader and the PartEvent API."*

This project implements the fourth row, and keeps the other three as runnable counter-examples.

---

## Run it

Requires a JDK (built and tested on JDK 25). Maven is not needed — the wrapper downloads it.

```bash
./mvnw spring-boot:run                 # Windows: mvnw.cmd spring-boot:run
```

Then open <http://localhost:8080> for a small upload page with a progress bar, or use curl.

### curl

The **metadata part must come before the file part**, and must be a plain form field (no `@`, no
`type=`): the server parses parts in order in a single pass, so there is no second chance to look it up.

```bash
# a file to play with: 512MB of zeros
head -c 536870912 /dev/zero > big.bin        # macOS/Linux
fsutil file createnew big.bin 536870912      # Windows

curl -X POST http://localhost:8080/api/upload \
  -F 'metadata={"filename":"big.bin","contentType":"application/octet-stream"}' \
  -F 'file=@big.bin'
```

```json
{"id":"0cc7d19b-...","filename":"big.bin","size":536870912,
 "sha256":"9acca8e8c22201155389f65abbf6bc9723edc7384ead80503839f49dcc56d767",
 "elapsedMillis":14351}
```

The digest is computed during the same single pass, so it costs nothing extra — if the bytes on disk
ever differ from what was sent, you find out immediately.

---

## The proof

`LargeUploadMemoryIT` starts the packaged jar as a **separate process** with `-Xmx256m`, uploads a
payload generated on the fly (no giant temp file anywhere), and checks the result byte for byte:

| Endpoint | Payload at a 256MB heap | Result |
|---|---|---|
| `POST /api/upload` (`Flux<PartEvent>`) | 512MB | **201**, digest matches, heap never comes close |
| `POST /api/legacy/join` (`join()` bridge) | 512MB | **OutOfMemoryError**, process exits |

Both tests upload the same bytes with the same heap; only the endpoint changes. The test also asserts
that no Netty buffer leak is reported while running with `-Dio.netty.leakDetection.level=paranoid`.

There is a second, sharper test in `WritesWhileUploadingTest`: the client sends **one** chunk and then
refuses to send the rest until it observes bytes on disk on the server side. A buffering server cannot
pass it — it would never write anything before the body completes, and the client would wait forever.
The streaming endpoint passes it with ~59KB on disk while the other 4MB is still unsent.

---

## How it works

```
POST /api/upload
   └── @RequestBody Flux<PartEvent>                       PartEventHttpMessageReader: no full parse
        └── windowUntil(PartEvent::isLast)                one window per part
             └── concatMap(...)                           parts handled strictly in order
                  ├── metadata (form field) → JSON → UploadMetadata
                  └── file → StorageBackend.store(Flux<DataBuffer>)
                       └── size ceiling + SHA-256 (one pass)
                            └── copy to heap
                                 └── DataBufferUtils.write(..., AsynchronousFileChannel)
                                      └── <uuid>.part  ──close+rename──▶  <uuid>
```

Four things in that picture are easy to get wrong, and each has a comment in the code explaining it:

- **Buffer release.** `DataBufferUtils.write` does *not* release the buffers it writes — it re-emits
  them. Whoever subscribes must release them, or pooled off-heap memory leaks. This is the single most
  common bug in hand-rolled streaming code.
- **Partial files.** When the stream fails or the client disconnects, the bytes already written stay on
  disk. The temp file is deleted on error *and* on cancellation, and the final file appears only via an
  atomic rename, so a half-written upload is never visible under the key a caller was given.
- **Pooled direct buffers.** Handing Netty's buffers straight to a file channel does not work on every
  platform — see the note below. Each chunk is copied to the heap first, which costs one memcpy apiece.
- **Early rejection.** The size ceiling is checked as bytes flow past, so an oversized upload is
  rejected mid-stream instead of after the whole file has been received. The trade-off is real and
  worth knowing: when you abort mid-upload, the client may see the connection drop before your error
  body arrives. Reject cheap things before the body starts, and drain before erroring if the caller
  needs a clean response.

### Which knob does what

Getting this wrong is easy, because several settings sound like they should bound the upload and don't.

| Setting | What it really controls | Effect on a streamed upload |
|---|---|---|
| `upload.decoder-chunk-size` | Reactor Netty's `maxChunkSize` | **The throughput knob.** How much body arrives per part event |
| `spring.webflux.multipart.max-in-memory-size` | `@RequestPart` spill threshold; form-field size for `PartEvent` | Nothing — the file stream is not held anywhere |
| `spring.webflux.multipart.max-disk-usage-per-part` | Ceiling per part on both readers, enforced mid-stream | This is the framework's hard limit → 413 |
| `spring.webflux.multipart.max-parts` | Number of parts in the request | Not a size limit |
| `upload.max-bytes` | Our own ceiling, enforced as bytes flow past | Our 413, with a useful problem detail |
| `spring.http.codecs.max-in-memory-size` | Aggregated decodes (JSON, `byte[]`, `@RequestPart Flux<DataBuffer>`) | Only matters for the legacy endpoints |

There are two ceilings on purpose: the framework's is set slightly higher so that ours trips first and
the caller gets a meaningful message rather than a generic rejection.

### Limits that bit us during development

- **`spring.codec.max-in-memory-size` no longer exists** in Boot 4 — it is `spring.http.codecs.max-in-memory-size`.
  Tutorials using the old name silently do nothing.
- **A `metadata` part sent with an explicit content type** (`-F 'metadata={...};type=application/json'`,
  which most tutorials show) is *not* a form field as far as Spring is concerned, so it does not arrive
  as a `FormPartEvent`. The endpoint tolerates both forms and bounds the size either way.
- **Writing pooled direct buffers through `AsynchronousFileChannel` fails on Windows with a current JDK**
  with `UnsupportedOperationException: ByteBuffer derived from closeable shared sessions not supported`.
  The channel wants a stable address; these buffers do not have one. Copying each chunk to the heap
  fixes it and works everywhere.

---

## The deliberately broken endpoints

`/api/legacy/**` reproduces the failure modes so you can see them rather than take my word for it.
They are enabled by default (`upload.legacy-endpoints-enabled`) — **do not deploy them**; one of them
can take the process down by design.

```bash
curl -F 'metadata={"filename":"a.bin"};type=application/json' -F 'file=@a.bin' \
     http://localhost:8080/api/legacy/request-part-flux
```

| Endpoint | What it demonstrates | Pinned by |
|---|---|---|
| `/api/legacy/request-part-flux` | The article's v1 signature. Joins the part into one buffer capped at 256KB → **413** for a 1MB file | `LegacyEndpointsTest` |
| `/api/legacy/block` | The v1 snippet with `.block()`. For a file under the 256KB aggregation cap it reaches the block and fails with **500**: *"block()/blockFirst()/blockLast() are blocking, which is not supported in thread reactor-http-nio-N"*. Above the cap you never get that far — the 413 arrives first, which is its own kind of damning | `LegacyEndpointsTest` |
| `/api/legacy/join` | The bridge with the illegal blocking removed. **Works** — right up until the file exceeds the heap, which is exactly why it is a trap | `LegacyEndpointsTest`, `LargeUploadMemoryIT` |
| `/api/legacy/join` without a metadata content type | **415**, not 400: binding `@RequestPart` to a type needs the part's content type | `LegacyEndpointsTest` |

---

## Performance notes

Measured on the machine this was developed on (Windows 11, JDK 25, single connection, loopback),
uploading 512MB through the streaming endpoint with `-Xmx256m`:

| Configuration | Time | Part events |
|---|---|---|
| Default decoder chunks (8KB) | ~29s | 65,538 |
| `upload.decoder-chunk-size: 256KB` (default here) | **~14.5s** | 8,195 |

For scale, raw `dd` write throughput to the same directory on that machine is ~81MB/s, so the
end-to-end number is dominated by disk and hashing rather than by the streaming design.

Two things found by measuring rather than assuming, both of which are now reflected in the code:

- **Chunk size dominates; write batching does not.** Gathering several chunks per write to cut syscalls
  changed the time by under 5%, so it was removed.
- **That batching was also incorrect.** `bufferTimeout` emits on a timer independent of downstream
  demand, while the file writer requests one write at a time; under load it produced
  `OverflowException: Could not emit buffer due to lack of requests`. The tests missed it because it is
  timing-dependent — the curl reproducer in this README is what caught it. The remaining code path is
  strictly demand-driven, which is also what makes backpressure real.

---

## Layout

```
src/main/java/dev/denis/hugeupload/
├── api/          UploadController (@RequestBody Flux<PartEvent>), problem-detail error mapping, DTOs
├── service/      PartEventUploadService (per-part demux), UploadMetadataCodec
├── storage/      StorageBackend + FileSystemStorageBackend (the streaming write path)
├── legacy/       the labelled anti-patterns and their blocking InputStream store
├── config/       UploadProperties, NettyDecoderConfig
└── util/         Filenames (client filenames are input, never paths)
```

Storage keys are server-generated UUIDs. A client-supplied filename is only ever echoed back or
logged — it never touches a path.

## Requirements

JDK 21+ (built and tested on 25). No Maven install required; the wrapper fetches Maven 3.9.16, verified
against a pinned SHA-256.

## What's next

Part 2 takes the same stream and puts a pipeline behind it: S3 multipart upload, file-type detection
from the first few hundred bytes instead of the whole file, and antivirus scanning that a huge file
cannot choke — plus the fan-out that keeps all of it on a single read of the request.

The servlet-stack counterpart — the same guarantees without WebFlux, for applications that cannot move
off blocking JPA/JDBC — is covered in the paid follow-up, alongside the Quarkus implementation.
