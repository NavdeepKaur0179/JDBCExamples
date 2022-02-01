/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sg.jdbctcomplexexample.dao;

import com.sg.jdbctcomplexexample.entity.Room;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 *
 * @author kaurn
 */
@Repository
public class RoomDaoDB implements RoomDao{
    
    @Autowired
    JdbcTemplate jdbc;
            
    @Override
    public List<Room> getAllRooms() {
        final String SELECT_ALL_ROOMS="SELECT * FROM room";
        return jdbc.query(SELECT_ALL_ROOMS, new RoomMapper());
    }

    @Override
    public Room getRoomById(int id) {
        try {
             final String SELECT_ROOM_BY_ID="SELECT * FROM room WHERE id=?";
             return jdbc.queryForObject(SELECT_ROOM_BY_ID,new RoomMapper(), id);            
        } catch (DataAccessException e) {
            return null;
        }       
    }

    @Override
    @Transactional   
    public Room addRoom(Room room) {
        final String INSERT_ROOM="INSERT INTO room(name,description) VALUES(?,?)";
        jdbc.update(INSERT_ROOM, 
                room.getName(),
                room.getDescription());
        int newInt=jdbc.queryForObject("SELECT LAST_INSERT_ID()", Integer.class);
        room.setId(newInt);
        return room;
    }

    @Override
    public void updateRoom(Room room) {
        final String UPDATE_ROOM="UPDATE room SET name=? ,description=? where id=?";
        jdbc.update(UPDATE_ROOM, 
                room.getName(),
                room.getDescription(),
                room.getId());
    }

    @Override
    @Transactional
    public void deleteRoomById(int id) {
        final String DELETE_MEETING_EMPLOYEE_BY_ROOM="DELETE me.* FROM meeting_employee me"+
                " JOIN meeting m on m.id=me.meetingId where m.roomId=?";
        jdbc.update(DELETE_MEETING_EMPLOYEE_BY_ROOM,id);
        
        final String DELETE_MEETING_BY_ROOM="DELETE m.* FROM meeting m"+
                " JOIN room r on r.id=m.roomID where m.roomId=?";
        jdbc.update(DELETE_MEETING_BY_ROOM, id);
        
        final String DELETE_ROOM="DELETE FROM room WHERE id=?";
        jdbc.update(DELETE_ROOM,id);
    }
    public static final class RoomMapper implements RowMapper<Room>
    {

        @Override
        public Room mapRow(ResultSet res, int index) throws SQLException {
            Room rm=new Room();
            rm.setId(res.getInt("id"));
            rm.setDescription(res.getString("description"));
            rm.setName(res.getString("name"));
            return rm;
        }
        
    }
            
}
