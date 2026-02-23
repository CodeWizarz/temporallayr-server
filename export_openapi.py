import json
from app.main import app


def generate_openapi():
    print("Exporting OpenAPI Schema natively from FastApi App...")
    openapi_schema = app.openapi()

    with open("openapi.json", "w") as f:
        json.dump(openapi_schema, f, indent=2)

    print("Done! openapi.json generated successfully.")


if __name__ == "__main__":
    generate_openapi()
