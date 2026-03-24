from __future__ import annotations

from pathlib import Path


class RupiProcessor:
    EXTENSIONS = {".tif", ".tiff"}

    def __init__(self, output_dir="/tmp/ftp_output", scale_percent=50):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.scale_percent = scale_percent

    def can_process(self, local_path):
        return local_path.suffix.lower() in self.EXTENSIONS

    def build_output_path(self, local_path):
        return self.output_dir / f"{local_path.stem}.png"

    def _require_image(self):
        try:
            from PIL import Image
        except ImportError as exc:
            raise RuntimeError("Pillow가 필요합니다. `pip install pillow`로 설치하세요.") from exc
        return Image

    def process(self, local_path):
        Image = self._require_image()
        output_path = self.build_output_path(local_path)
        with Image.open(local_path) as image:
            width, height = image.size
            new_size = (
                max(1, round(width * self.scale_percent / 100)),
                max(1, round(height * self.scale_percent / 100)),
            )
            resized = image.resize(new_size)
            resized.save(output_path, format="PNG")
            print(
                f"[IMAGE] {local_path.name} / size={image.size} / mode={image.mode} "
                f"-> output={output_path} / resized={new_size}"
            )
        return output_path
