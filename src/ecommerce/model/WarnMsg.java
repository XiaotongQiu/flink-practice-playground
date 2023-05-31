package ecommerce.model;

public class WarnMsg {
    private String userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String msg;

    public WarnMsg() {
    }

    public WarnMsg(String userId, Long firstFailTime, Long lastFailTime, String msg) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "WarnMsg{" +
                "userId='" + userId + '\'' +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", msg='" + msg + '\'' +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getFirstFailTime() {
        return firstFailTime;
    }

    public void setFirstFailTime(Long firstFailTime) {
        this.firstFailTime = firstFailTime;
    }

    public Long getLastFailTime() {
        return lastFailTime;
    }

    public void setLastFailTime(Long lastFailTime) {
        this.lastFailTime = lastFailTime;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
