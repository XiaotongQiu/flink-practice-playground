package ecommerce.model;

public class MarketingUserBehavior {
    private String userId;
    private String behavior;
    private String channel;
    private Long ts;

    public MarketingUserBehavior() {
    }

    public MarketingUserBehavior(String userId, String behavior, String channel, Long ts) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.ts = ts;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "MarketingUserBehavior{" +
                "userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", ts=" + ts +
                '}';
    }
}
