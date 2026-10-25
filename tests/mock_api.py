"""Servidor mock minimo con la forma de Socrata SODA (/resource/<id>.json con $limit/$offset).
Modos para probar la politica de errores: ok | flaky (503 las primeras N peticiones) | forbidden | badjson.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

MUNICIPIOS = ["05001", "11001", "76001", "13001", "08001", "47001", "68001", "63001"]


def make_records(years=range(2019, 2027), municipios=None):
    municipios = municipios or MUNICIPIOS
    return [
        {"cod_municipio": m, "ano": str(y), "poblacion_total": str(100000 + i * 1000 + y - 2000), ":id": f"{m}-{y}"}
        for i, m in enumerate(municipios) for y in years
    ]


def start(mode="ok", fail_first=0, port=0, municipios=None):
    state = {"calls": 0, "mode": mode}
    data = make_records(municipios=municipios)

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):  # silencio
            pass

        def do_GET(self):
            state["calls"] += 1
            q = parse_qs(urlparse(self.path).query)
            if state["mode"] == "forbidden":
                return self._send(403, {"error": "forbidden"})
            if state["mode"] == "badjson":
                self.send_response(200); self.end_headers(); self.wfile.write(b"<html>no json</html>"); return
            if state["mode"] == "flaky" and state["calls"] <= fail_first:
                return self._send(503, {"error": "unavailable"})
            limit = int(q.get("$limit", ["1000"])[0]); off = int(q.get("$offset", ["0"])[0])
            self._send(200, data[off: off + limit])

        def _send(self, code, payload):
            body = json.dumps(payload).encode()
            self.send_response(code); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    srv = HTTPServer(("127.0.0.1", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, state, f"http://127.0.0.1:{srv.server_port}"
