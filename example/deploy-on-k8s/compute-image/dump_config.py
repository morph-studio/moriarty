from pathlib import Path

from moriarty.deploy.endpoint import Endpoint

_HERE = Path(__file__).parent

if __name__ == "__main__":
    define = Endpoint(endpoint_name="hello-world", image="wh1isper/moriarty-compute-image:latest")
    dump_path = _HERE / "config.json"
    dump_path.write_text(define.model_dump_json(indent=4))
    print(f"Dumped config to {dump_path.as_posix()}")
