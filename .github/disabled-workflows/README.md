# Publishing paused

These inherited workflows are outside `.github/workflows`, so GitHub Actions cannot trigger them.
SDK-PR5 replaces them with a reviewed release flow for the new `nzovu` package.
The local `make publish` and `make publish-test` targets also fail without publishing.
