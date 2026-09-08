from time import perf_counter

from django.conf import settings
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from .models import Order, Product
from django.http import HttpResponse


def products(request):
    started = perf_counter()
    context = {"products": Product.objects.order_by("id")}
    status = 200
    if request.method == "POST":
        product_id = request.POST.get("product_id", "")
        quantity_text = request.POST.get("quantity", "")
        product = Product.objects.filter(pk=product_id).first()
        try:
            quantity = int(quantity_text)
        except (TypeError, ValueError):
            quantity = 0

        if product is None:
            error = "주문할 메뉴를 선택해 주세요."
        elif not 1 <= quantity <= 2147483647:
            error = "수량은 1 이상 2,147,483,647 이하의 정수로 입력해 주세요."
        else:
            order = Order.objects.create(
                product=product,
                quantity=quantity,
                unit_price=product.unit_price,
                ordered_at=timezone.now(),
            )
            return redirect(f"{reverse('shop:orders')}?received={order.pk}")

        context.update(
            order_error=error,
            selected_product_id=product_id,
            order_quantity=quantity_text,
        )
        status = 400

    response = render(request, "shop/products.html", context, status=status)
    if request.method == "GET":
        duration_ms = int((perf_counter() - started) * 1000)
        timestamp = timezone.localtime().isoformat(timespec="seconds")
        with (settings.DATA_DIR / "raw" / "access.log").open("a", encoding="utf-8") as stream:
            stream.write(f"{timestamp} GET /products/ 200 {duration_ms}\n")
    return response


def orders(request):
    recent_orders = list(Order.objects.select_related("product").order_by("-id")[:50])
    received_id = request.GET.get("received")
    received_order = next((order for order in recent_orders if str(order.pk) == received_id), None)
    return render(
        request,
        "shop/orders.html",
        {
            "orders": recent_orders,
            "received_order": received_order,
            "order_quantity_total": sum(order.quantity for order in recent_orders),
            "order_amount_total": sum(order.amount for order in recent_orders),
        },
    )

def formview(request):
    return render(request, "shop/form.html")

@csrf_exempt
def formprocess(request):
    data = request.POST.get("test1")
    data2 = request.POST.get("kkw")
    print("당신이 보낸 데이터 :", data)
    print("당신이 보낸 데이터2 :", data2)
    return HttpResponse("데이터를 정상적으로 받았습니다.")