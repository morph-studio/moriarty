from fastapi import FastAPI, Request, Response, status

app = FastAPI()


@app.get("/ping")
async def root():
    """Leave it for health check"""
    return Response(status_code=status.HTTP_200_OK)


@app.post("/invocations")
async def root(
    request: Request,
):
    return await request.json()
