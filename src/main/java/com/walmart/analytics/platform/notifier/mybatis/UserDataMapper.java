package com.walmart.analytics.platform.notifier.mybatis;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

import javax.inject.Named;
import java.util.List;
import java.util.Map;

@Mapper
@Named
public interface UserDataMapper {

    String TABLE = "NOTIFICATION_USER_GROUPS";
    String USER_GROUP_QUERY = "SELECT USER_GROUP_QUERY " +
        "FROM " + TABLE + " " +
        "WHERE USER_GROUP = #{userGroup} AND STATUS = 'ACTIVE'";
    String NEW_USER_GRP_QUERY = "INSERT INTO " + TABLE +
        " (USER_GROUP, USER_GROUP_QUERY, STATUS, CREATED_BY, LAST_UPDATED_BY) " +
        " VALUES (#{userGrp}, #{userGrpQuery}, 'ACTIVE', #{user}, #{user})";
    String EDIT_USER_GRP_QUERY = "UPDATE " + TABLE
        + " SET USER_GROUP = #{newGroupName}, QUERY = #{query}, LAST_UPDATED_BY = #{user}"
        + " WHERE USER_GROUP = #{oldGroupName}";

    String DELETE_USER_GRP_QUERY = "UPDATE " + TABLE
        + " SET STATUS = 'DELETED', LAST_UPDATED_BY = #{user}"
        + " WHERE USER_GROUP = #{groupName}";


    @Select(USER_GROUP_QUERY)
    String getUserGroupQuery(@Param("userGroup") String userGroup);

    @SelectProvider(type = UserGroupQueryGenerator.class, method = "userGroupQuery")
    List<String> getUsers(String query, Map<String, Object> params);

    @Insert(NEW_USER_GRP_QUERY)
    int createUserGroup(@Param("userGrp") String userGroupName,
                        @Param("userGrpQuery") String userGroupQuery,
                        @Param("user") String user);

    @Update(EDIT_USER_GRP_QUERY)
    int updateUserGroup(@Param("oldGroupName") String oldGroupName,
                        @Param("newGroupName") String newGroupName,
                        @Param("query") String query,
                        @Param("user") String user);

    @Update(DELETE_USER_GRP_QUERY)
    int deleteUserGroup(@Param("groupName") String groupName,
                        @Param("user") String user);

}
