package com.cassandraplayground.blog;

import com.cassandraplayground.notifications.Constants;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@Table(keyspace = Constants.KEYSPACE, name = "posts")
public class Post extends AbstractVO<Post> {
    @PartitionKey
    private UUID id;
    @Column(name="blog_id")
    private UUID blogId;
    @Column(name = "posted_on")
    private long postedOn;
    private String title;
    private String content;
    private Set<String> tags;

    public UUID getId() {
        return id;
    }
    public void setId(UUID id) {
        this.id = id;
    }
    public UUID getBlogId() {
        return blogId;
    }
    public void setBlogId(UUID blogId) {
        this.blogId = blogId;
    }
    public long getPostedOn() {
        return postedOn;
    }
    public void setPostedOn(long postedOn) {
        this.postedOn = postedOn;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public String getContent() {
        return content;
    }
    public void setContent(String content) {
        this.content = content;
    }
    public Set<String> getTags() {
        return tags;
    }
    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    @Override
    protected Post getInstance() {
        return this;
    }
    @Override
    protected Class<Post> getType() {
        return Post.class;
    }

    @Override
    public void save(CassandraConnection.SessionWrapper sessionWrapper) {
        super.save(sessionWrapper);
        for (String tag: getTags()){
            Category cat = new Category();
            cat.setBlogId(getBlogId());
            cat.setPostId(getId());
            cat.setCategoryName(tag);
            cat.setPostTitle(getTitle());
            cat.save(sessionWrapper);
        }
    }

    // ---- ACCESSORS ----

    public static List<Post> getPosts(UUID blogId, UUID fromPost, int pageSize, CassandraConnection.SessionWrapper sessionWrapper){
        com.cassandraplayground.blog.AllQueries queries =  sessionWrapper.getAllQueries();
        if (fromPost != null){
            return queries.getPostsReverseChronologically(blogId, fromPost, pageSize).all();
        } else {
            return queries.getPostsReverseChronologically(blogId, pageSize).all();
        }
    }

    public static List<Post> getPosts(UUID blogId, int pageSize, CassandraConnection.SessionWrapper sessionWrapper){
        return getPosts(blogId, null, pageSize, sessionWrapper);
    }
}