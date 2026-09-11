import json
from pathlib import Path
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = "현재 주문을 내보내고 최종 Delta 배치로 대시보드를 갱신합니다."

    def add_arguments(self, parser):
        parser.add_argument("--cores", type=int, choices=[1, 2], default=2)
        parser.add_argument("--data-dir", default=str(settings.DATA_DIR))

    def handle(self, *args, **options):
        summary_path = Path(options["data_dir"]).resolve() / "marts" / "dashboard.json"
        before = json.loads(summary_path.read_text(encoding="utf-8"))
        self.stdout.write(f"갱신 전 분석 주문: {before['order_count']}건")
        call_command("export_analytics", data_dir=options["data_dir"])
        call_command("run_spark_batch", cores=options["cores"], data_dir=options["data_dir"], delta=True)
        after = json.loads(summary_path.read_text(encoding="utf-8"))
        self.stdout.write(f"갱신 후 분석 주문: {after['order_count']}건")
        self.stdout.write("대시보드를 새로고침하세요: http://127.0.0.1:8000/dashboard/")
    

