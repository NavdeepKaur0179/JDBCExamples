/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sg.jdbctcomplexexample.dao;

import com.sg.jdbctcomplexexample.dao.EmployeeDaoDB.EmployeeMapper;
import com.sg.jdbctcomplexexample.dao.RoomDaoDB.RoomMapper;
import com.sg.jdbctcomplexexample.entity.Employee;
import com.sg.jdbctcomplexexample.entity.Meeting;
import com.sg.jdbctcomplexexample.entity.Room;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
public class MeetingDaoDB implements MeetingDao {

    @Autowired
    private JdbcTemplate jdbc;

    private List<Employee> getEmployeesForMeeting(Meeting meeting) {
        final String SELECT_EMPLOYEES_FOR_MEETING = "SELECT e.* FROM employee e"
                + " JOIN meeting_employee me ON me.employeeId=e.id WHERE me.meetingId=?";
        return jdbc.query(SELECT_EMPLOYEES_FOR_MEETING, new EmployeeMapper(), meeting.getId());
    }

    private Room getaRoomForMeeting(Meeting meeting) {
        final String SELECT_ROOM_FOR_MEETING = "SELECT r.* FROM room r"
                + " JOIN meeting m ON m.roomId=r.id WHERE m.id=?";
        return jdbc.queryForObject(SELECT_ROOM_FOR_MEETING, new RoomMapper(), meeting.getId());
    }

    private void getRoomAndmEmployeesForMeetings(List<Meeting> meetings) {
        for (Meeting meeting : meetings) {
            meeting.setAttendees(getEmployeesForMeeting(meeting));
            meeting.setRoom(getaRoomForMeeting(meeting));
        }
    }

    private void insertMeetingEmployees(Meeting meeting) {
        final String INSERT_MEETING_EMPLOYEE = "INSERT INTO meeting_employee(meetingId, employeeId) VALUES(?,?)";
        for (Employee employee : meeting.getAttendees()) {
            jdbc.update(INSERT_MEETING_EMPLOYEE,
                    meeting.getId(),
                    employee.getId());
        }
    }

    public static final class MeetingMapper implements RowMapper<Meeting> {

        @Override
        public Meeting mapRow(ResultSet res, int index) throws SQLException {
            Meeting meeting = new Meeting();
            meeting.setId(res.getInt("id"));
            meeting.setName(res.getString("name"));
            meeting.setTime(res.getTimestamp("time").toLocalDateTime());
            return meeting;
        }
    }

    @Override
    public List<Meeting> getAllMeetings() {
        final String SELECT_ALL_MEETINGS = "SELECT * FROM meeting";
        List<Meeting> meetings = jdbc.query(SELECT_ALL_MEETINGS, new MeetingMapper());
        getRoomAndmEmployeesForMeetings(meetings);
        return meetings;
    }

    @Override
    public Meeting getMeetingByid(int id) {
        try {
            final String SELECT_MEETING_BY_ID = "SELECT * FROM meeting WHERE id=?";
            Meeting meeting = jdbc.queryForObject(SELECT_MEETING_BY_ID, new MeetingMapper(), id);
            meeting.setAttendees(getEmployeesForMeeting(meeting));
            meeting.setRoom(getaRoomForMeeting(meeting));
            return meeting;
        } catch (DataAccessException e) {
            return null;
        }

    }

    @Override
    @Transactional
    public Meeting addMeeting(Meeting meeting) {
        final String INSERT_MEETING = "INSERT into meeting(name, time, roomId) VALUES(?,?,?)";
        jdbc.update(INSERT_MEETING, new MeetingMapper(),
                meeting.getName(),
                Timestamp.valueOf(meeting.getTime()),
                meeting.getRoom()
        );
        int newId = jdbc.queryForObject("SELECT LAST_INSERT_ID", Integer.class);
        meeting.setId(newId);
        insertMeetingEmployees(meeting);
        return meeting;
    }

    @Override
    @Transactional
    public void updateMeeting(Meeting meeting) {
        final String UPDATE_MEETING = "UPDATE meeting SET name=?,"
                + "time=?,roomId=? where id=?";
        jdbc.update(UPDATE_MEETING,
                meeting.getName(),
                Timestamp.valueOf(meeting.getTime()),
                meeting.getRoom(),
                meeting.getId());
        final String DELETE_MEETING_EMPLOYEE = "DELETE * FROM meeting_employee WHERE meeetingId=?";
        jdbc.update(DELETE_MEETING_EMPLOYEE, meeting.getId());
        insertMeetingEmployees(meeting);
    }

    @Override
    public void deleteMeetingById(int id) {
        final String DELETE_MEETING_EMPLOYEE = "DELETE FROM meeting_employee "
                + "WHERE meetingId = ?";
        jdbc.update(DELETE_MEETING_EMPLOYEE, id);

        final String DELETE_MEETING = "DELETE FROM meeting WHERE id = ?";
        jdbc.update(DELETE_MEETING, id);
    }

    @Override
    public List<Meeting> getMeetingsForRoom(Room room) {
        final String SELECT_MEETINGS_BY_ROOM = "SELECt m.* FROM meeting WHERE m.roomId=?";
        List<Meeting> meetings = jdbc.query(SELECT_MEETINGS_BY_ROOM,
                new MeetingMapper(),
                room.getId());
        getRoomAndmEmployeesForMeetings(meetings);
        return meetings;
    }

    @Override
    public List<Meeting> getMeetingsForEmployee(Employee employee) {
        final String SELECT_MEETINGS_BY_EMPLOYEE = "SELECT m.* FROM meeting"
                + " JOIN  meeting_employee me ON m.id=me.meetingId WHERE me.employeeId=?";
        List<Meeting> meetings = jdbc.query(SELECT_MEETINGS_BY_EMPLOYEE, new MeetingMapper(), employee.getId());
        getRoomAndmEmployeesForMeetings(meetings);
        return meetings;
    }

}
