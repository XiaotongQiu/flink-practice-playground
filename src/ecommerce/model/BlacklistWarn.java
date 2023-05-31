package ecommerce.model;

public class BlacklistWarn {
    private Long userId;
    private Long adId;
    private String msg;

    public BlacklistWarn() {
    }

    public BlacklistWarn(Long userId, Long adId, String msg) {
        this.userId = userId;
        this.adId = adId;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "BlacklistWarn{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", msg='" + msg + '\'' +
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
