package ecommerce.model;

public class CountByProvince {
    private String province;
    private Long windowEnd;
    private Long count;

    public CountByProvince() {
    }

    public CountByProvince(String province, Long windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "CountByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
