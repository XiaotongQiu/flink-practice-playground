package ecommerce.model;

public class UserBahavior {
    private String userId;
    private String itemId;
    private String categoryId;
    private String behaviour;
    private Long timestamp;

    @Override
    public String toString() {
        return "UserBahavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behaviour='" + behaviour + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public UserBahavior() {
    }

    public UserBahavior(String userId, String itemId, String categoryId, String behaviour, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behaviour = behaviour;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehaviour() {
        return behaviour;
    }

    public void setBehaviour(String behaviour) {
        this.behaviour = behaviour;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
