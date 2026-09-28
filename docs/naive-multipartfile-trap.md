# The `MultipartFile` trap, as a runnable snippet

This is the "before" picture from Part 1. It is deliberately **not** a module in this build: it is a
Spring MVC application, and the point of Part 1 is the WebFlux path. Paste this into a scratch
project if you want to watch it fail — the reproduction steps are below.

## The code every tutorial shows

```java
@RestController
public class NaiveUploadController {

    private final Path storageRoot;

    public NaiveUploadController(@Value("${storage.root}") Path storageRoot) {
        this.storageRoot = storageRoot;
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> upload(@RequestPart("file") MultipartFile file,
                                      @RequestPart("metadata") UploadMetadata metadata) throws IOException {
        Path target = storageRoot.resolve(UUID.randomUUID().toString());
        file.transferTo(target);                       // the file is already whole by the time we get here
        return Map.of("id", target.getFileName().toString(), "size", file.getSize());
    }
}
```

```yaml
spring:
  servlet:
    multipart:
      max-file-size: 5GB          # every tutorial raises these two, because the 1MB default 413s
      max-request-size: 5GB
      file-size-threshold: 0      # 0 = the whole body is spooled to a temp file (the Tomcat default)
```

## What actually happens

By the time `upload(...)` is called, the entire request body has already been received and stored
somewhere. The handler is not a stream — it is a receipt.

Two configurations, two ways to lose:

| `file-size-threshold` | Where the body goes | The failure |
|---|---|---|
| `0` (Tomcat default) | A temp file under `java.io.tmpdir` | **Whole file on disk per concurrent upload**, and nothing reaches your storage until the last byte. Ten concurrent 2GB uploads is 20GB of temp disk you did not plan for |
| Larger than the file | The heap | **`OutOfMemoryError`.** A 1.5GB upload against `-Xmx512m` kills the JVM, taking every other in-flight request with it |

`MultipartFile` is not a view over the request; it is a handle to something that has already finished
arriving. That is why there is no way to start writing to storage at byte zero, no way to reject a file
on your own rules until you have paid for all of it, and no way to bound the cost per concurrent
upload at anything smaller than the file.

## Reproducing it

```bash
# 1. a scratch Spring MVC app with the controller and yaml above, then run it small:
java -Xmx512m -jar naive-upload.jar

# 2. a file bigger than the heap
head -c 1610612736 /dev/zero > big.bin        # 1.5GB   (Windows: fsutil file createnew big.bin 1610612736)

# 3. for the heap configuration
curl -X POST http://localhost:8080/upload \
  -F 'metadata={"filename":"big.bin"};type=application/json' \
  -F 'file=@big.bin'
# → java.lang.OutOfMemoryError: Java heap space, and the process is gone

# 4. for the default (disk) configuration, watch the temp directory instead
ls -l "$TMPDIR"        # the whole file is there before your handler logs anything
```

## Two honest caveats

- **The container can reject early on its own limits.** Tomcat enforces `max-file-size` during parsing,
  so an oversized upload is refused before it lands. What you lose is not all rejection — it is *your*
  rejection: no quota check, no content-type policy, no persistence until the last byte has been
  spooled. Part 2 is about exactly those checks, done cheaply and early.
- **The servlet stack can stream.** `request.getInputStream()` is a real stream, and a servlet
  application can persist bytes as they arrive without WebFlux. It is a different shape of solution
  (bounded buffers, its own reject-and-clean-up logic, its own backpressure story) and it is covered in
  the paid follow-up, because plenty of teams cannot move to WebFlux and still need this to work.

The WebFlux equivalent of that "already whole by the time you see it" behaviour is `@RequestPart` —
including `FilePart`, which is a temp-file-backed part. See the README for what it does and the
`/api/legacy/**` endpoints for the evidence.
