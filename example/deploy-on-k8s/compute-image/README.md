## Simple example for building an image

```bash
docker build -t wh1isper/moriarty-compute-image:latest .
docker push wh1isper/moriarty-compute-image:latest
```

Then deploy it

```bash
moriarty-deploy deploy-or-update --endpoint-config-file config.json
```
