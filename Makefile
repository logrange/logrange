OS ?= `go env GOOS`
ARCH ?= `go env GOARCH`

BUILDDIR = build


.PHONY: build
build: collector


.PHONY: collector
collector:
	@GOOS=$(OS) GOARCH=$(ARCH) go build -o $(BUILDDIR)/$@ ./cmd/collector


.PHONY: clean
clean:
	@rm -rf $(BUILDDIR)
	@rm -rf `go env GOPATH`/pkg/$(OS)_$(ARCH)/github.com/logrange/logrange
