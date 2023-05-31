package ecommerce.model;

public class OrderResult {
    private String OrderId;
    private String msg;

    public OrderResult() {
    }

    public OrderResult(String orderId, String msg) {
        OrderId = orderId;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "OrderId='" + OrderId + '\'' +
                ", msg='" + msg + '\'' +
                '}';
    }

    public String getOrderId() {
        return OrderId;
    }

    public void setOrderId(String orderId) {
        OrderId = orderId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
