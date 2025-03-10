package com.xuxueli.springbootpriorityqueue.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * 排序任务模型类，用于有序队列示例
 * 
 * 该类表示一个可以在有序队列中排序的任务，包含以下属性：
 * - id: 任务唯一标识符
 * - name: 任务名称
 * - description: 任务描述
 * - priority: 任务优先级（数值越小优先级越高）
 * - createdAt: 任务创建时间
 */
public class SortedTask implements Serializable {
    
    private String id;
    private String name;
    private String description;
    private double priority;
    private LocalDateTime createdAt;
    
    /**
     * 默认构造函数
     */
    public SortedTask() {
        this.createdAt = LocalDateTime.now();
    }
    
    /**
     * 带参数的构造函数
     * 
     * @param id 任务ID
     * @param name 任务名称
     * @param description 任务描述
     * @param priority 任务优先级
     */
    public SortedTask(String id, String name, String description, double priority) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.priority = priority;
        this.createdAt = LocalDateTime.now();
    }
    
    // Getters and Setters
    
    /**
     * 获取任务ID
     * @return 任务ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * 设置任务ID
     * @param id 任务ID
     */
    public void setId(String id) {
        this.id = id;
    }
    
    /**
     * 获取任务名称
     * @return 任务名称
     */
    public String getName() {
        return name;
    }
    
    /**
     * 设置任务名称
     * @param name 任务名称
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * 获取任务描述
     * @return 任务描述
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * 设置任务描述
     * @param description 任务描述
     */
    public void setDescription(String description) {
        this.description = description;
    }
    
    /**
     * 获取任务优先级
     * @return 任务优先级
     */
    public double getPriority() {
        return priority;
    }
    
    /**
     * 设置任务优先级
     * @param priority 任务优先级
     */
    public void setPriority(double priority) {
        this.priority = priority;
    }
    
    /**
     * 获取任务创建时间
     * @return 任务创建时间
     */
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    /**
     * 设置任务创建时间
     * @param createdAt 任务创建时间
     */
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    /**
     * 重写equals方法，基于ID比较对象是否相等
     * 
     * @param o 要比较的对象
     * @return 如果ID相同则返回true，否则返回false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortedTask task = (SortedTask) o;
        return Objects.equals(id, task.id);
    }
    
    /**
     * 重写hashCode方法，基于ID生成哈希码
     * 
     * @return 对象的哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    /**
     * 重写toString方法，提供对象的字符串表示
     * 
     * @return 对象的字符串表示
     */
    @Override
    public String toString() {
        return "SortedTask{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", priority=" + priority +
                ", createdAt=" + createdAt +
                '}';
    }
} 