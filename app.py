"""
DemoSync - Orquestador Central de Microservicios (SIMULACIÓN DE DEMOSTRACIÓN)
==============================================================================
Este archivo implementa una simulación de un orquestador de microservicios
usando Flask. En un entorno de producción real, el orquestador lanzaría
contenedores Docker (via Docker SDK o Kubernetes API). Aquí, simulamos
ese comportamiento generando URLs de callback y almacenando el estado
en memoria para demostrar el patrón de arquitectura.

Arquitectura Simulada:
  [Cliente] --> [DemoSync Orchestrator] --> [Microservicio Simulado (URL)]
                        |                          |
                        |<--- POST /api/recibir-datos (callback) <--|
                        |
                  [Estructura de datos en memoria]

Autor: DemoSync Demo
"""

import uuid
import os
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template, abort

# ---------------------------------------------------------------------------
# Inicialización de la aplicación Flask
# ---------------------------------------------------------------------------
app = Flask(__name__)

# ---------------------------------------------------------------------------
# "Base de datos" en memoria
# Estructura:
# tasks_db = {
#   "<taskId>": {
#       "userId":        str,
#       "serviceType":   str,
#       "status":        "running" | "finished",
#       "callback_url":  str,
#       "created_at":    str (ISO 8601),
#       "finished_at":   str | None,
#       "collected_data": [
#           {"data_field_1": ..., "data_field_2": ..., "received_at": ...},
#           ...
#       ]
#   }
# }
# ---------------------------------------------------------------------------
tasks_db: dict[str, dict] = {}

# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def get_base_url() -> str:
    """
    Determina la URL base de la aplicación.
    En producción (Render) usa la variable de entorno RENDER_EXTERNAL_URL.
    En desarrollo local usa localhost.
    """
    render_url = os.environ.get("RENDER_EXTERNAL_URL")
    if render_url:
        return render_url.rstrip("/")
    return "http://localhost:5000"


def now_iso() -> str:
    """Devuelve la fecha/hora actual en formato ISO 8601 UTC."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Endpoints principales
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Sirve la página principal de demostración."""
    return render_template("index.html", base_url=get_base_url())


# ---------------------------------------------------------------------------
# 1. Creación de Tarea (Orquestación)
# ---------------------------------------------------------------------------

@app.route("/api/orquestar-tarea", methods=["POST"])
def orquestar_tarea():
    """
    SIMULACIÓN: En producción, este endpoint lanzaría un contenedor Docker
    con el microservicio correspondiente a 'serviceType'. Aquí, generamos
    un taskId y una callback_url que representan ese "microservicio lanzado".

    Body JSON esperado:
      {
        "userId":      "user-123",
        "serviceType": "data-collector-microsoft" | "data-collector-instagram" | ...
      }

    Responde con:
      {
        "taskId":       "<uuid4>",
        "callback_url": "https://<host>/api/recibir-datos",
        "status":       "running",
        "message":      "..."
      }
    """
    payload = request.get_json(silent=True)

    # --- Validación de entrada ---
    if not payload:
        return jsonify({"error": "Se requiere un body JSON válido."}), 400

    user_id = payload.get("userId", "").strip()
    service_type = payload.get("serviceType", "").strip()

    if not user_id:
        return jsonify({"error": "El campo 'userId' es obligatorio."}), 400
    if not service_type:
        return jsonify({"error": "El campo 'serviceType' es obligatorio."}), 400

    # --- Generación del identificador único de tarea ---
    task_id = str(uuid.uuid4())

    # --- SIMULACIÓN: En producción aquí iría algo como:
    #     docker_client.containers.run(
    #         image=f"demosync/{service_type}:latest",
    #         environment={"TASK_ID": task_id, "CALLBACK_URL": callback_url},
    #         detach=True
    #     )
    # En su lugar, simplemente construimos la URL de callback que el
    # microservicio usaría para reportar sus datos al orquestador.
    # ---
    callback_url = f"{get_base_url()}/api/recibir-datos"

    # --- Almacenamiento del estado de la tarea en memoria ---
    tasks_db[task_id] = {
        "userId":         user_id,
        "serviceType":    service_type,
        "status":         "running",
        "callback_url":   callback_url,
        "created_at":     now_iso(),
        "finished_at":    None,
        "collected_data": [],
    }

    app.logger.info(
        f"[ORQUESTADOR] Tarea '{task_id}' creada para usuario '{user_id}' "
        f"con servicio '{service_type}'. (SIMULADO)"
    )

    return jsonify({
        "taskId":       task_id,
        "callback_url": callback_url,
        "status":       "running",
        "message":      (
            f"Microservicio '{service_type}' lanzado (SIMULADO). "
            f"Envía datos a la callback_url con el taskId para simular "
            f"la recolección de información."
        ),
    }), 201


# ---------------------------------------------------------------------------
# 2. Endpoint de Callback (Recepción de Datos del Microservicio)
# ---------------------------------------------------------------------------

@app.route("/api/recibir-datos", methods=["POST"])
def recibir_datos():
    """
    SIMULACIÓN: Este endpoint imita el callback que un microservicio real
    llamaría para reportar los datos recolectados al orquestador central.

    Acepta multipart/form-data O application/json con:
      - taskId       (obligatorio)
      - data_field_1 (opcional)
      - data_field_2 (opcional)
      - [cualquier campo adicional]

    Responde con 204 No Content si la tarea existe y está en estado 'running'.
    """
    # Soporte tanto para form-data como JSON (flexibilidad de demostración)
    if request.content_type and "application/json" in request.content_type:
        data = request.get_json(silent=True) or {}
    else:
        # multipart/form-data o application/x-www-form-urlencoded
        data = request.form.to_dict()

    task_id = data.get("taskId", "").strip()

    if not task_id:
        return jsonify({"error": "El campo 'taskId' es obligatorio."}), 400

    task = tasks_db.get(task_id)

    if task is None:
        return jsonify({"error": f"Tarea '{task_id}' no encontrada."}), 404

    if task["status"] != "running":
        return jsonify({
            "error": (
                f"La tarea '{task_id}' está en estado '{task['status']}' "
                f"y no puede recibir más datos."
            )
        }), 409

    # --- Extracción de campos de datos (excluye taskId del payload guardado) ---
    record = {k: v for k, v in data.items() if k != "taskId"}
    record["received_at"] = now_iso()

    task["collected_data"].append(record)

    app.logger.info(
        f"[CALLBACK] Datos recibidos para tarea '{task_id}': {record}"
    )

    # 204 No Content — estándar para callbacks/webhooks
    return "", 204


# ---------------------------------------------------------------------------
# 3. Dashboard de Estado y Sincronización
# ---------------------------------------------------------------------------

@app.route("/api/estado-tareas/<string:user_id>", methods=["GET"])
def estado_tareas(user_id: str):
    """
    Devuelve el resumen de todas las tareas orquestadas por un usuario
    específico, incluyendo los datos recolectados por cada una.

    Parámetro de ruta:
      user_id — identificador del usuario (ej. 'user-123')
    """
    user_tasks = {
        task_id: task_data
        for task_id, task_data in tasks_db.items()
        if task_data["userId"] == user_id
    }

    return jsonify({
        "userId":     user_id,
        "totalTasks": len(user_tasks),
        "tasks":      user_tasks,
    }), 200


@app.route("/api/admin/estado-global", methods=["GET"])
def estado_global():
    """
    [ADMIN] Devuelve el estado completo de TODAS las tareas y todos los
    datos recolectados. Demuestra la capacidad de supervisión centralizada
    del orquestador.

    En producción, este endpoint estaría protegido por autenticación.
    """
    total_running  = sum(1 for t in tasks_db.values() if t["status"] == "running")
    total_finished = sum(1 for t in tasks_db.values() if t["status"] == "finished")
    total_records  = sum(len(t["collected_data"]) for t in tasks_db.values())

    return jsonify({
        "summary": {
            "totalTasks":    len(tasks_db),
            "runningTasks":  total_running,
            "finishedTasks": total_finished,
            "totalRecords":  total_records,
        },
        "tasks": tasks_db,
    }), 200


# ---------------------------------------------------------------------------
# 4. Finalización de Tarea
# ---------------------------------------------------------------------------

@app.route("/api/finalizar-tarea", methods=["POST"])
def finalizar_tarea():
    """
    Marca una tarea como 'finished'. A partir de este punto, el endpoint
    de callback rechazará nuevos datos para esa tarea.

    Body JSON esperado:
      { "taskId": "<uuid4>" }

    SIMULACIÓN: En producción, esto también detendría/eliminaría el
    contenedor Docker asociado a la tarea.
    """
    payload = request.get_json(silent=True)

    if not payload:
        return jsonify({"error": "Se requiere un body JSON válido."}), 400

    task_id = payload.get("taskId", "").strip()

    if not task_id:
        return jsonify({"error": "El campo 'taskId' es obligatorio."}), 400

    task = tasks_db.get(task_id)

    if task is None:
        return jsonify({"error": f"Tarea '{task_id}' no encontrada."}), 404

    if task["status"] == "finished":
        return jsonify({
            "message": f"La tarea '{task_id}' ya estaba finalizada.",
            "task":    task,
        }), 200

    # --- SIMULACIÓN: docker_client.containers.get(task_id).stop() ---
    task["status"]      = "finished"
    task["finished_at"] = now_iso()

    app.logger.info(
        f"[ORQUESTADOR] Tarea '{task_id}' finalizada. "
        f"Registros recolectados: {len(task['collected_data'])}. (SIMULADO)"
    )

    return jsonify({
        "message":       f"Tarea '{task_id}' finalizada correctamente.",
        "taskId":        task_id,
        "status":        "finished",
        "totalRecords":  len(task["collected_data"]),
        "finished_at":   task["finished_at"],
    }), 200


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV", "production") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)