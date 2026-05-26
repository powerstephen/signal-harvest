import uvicorn
from config import HOST, PORT

if __name__ == "__main__":
    uvicorn.run("server.api:app", host=HOST, port=PORT, reload=False, log_level="info")
