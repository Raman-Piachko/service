package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.exception.DaoException;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.epam.rd.autocode.util.BigIntegerUtil.getBigInteger;

public class DepartmentDaoImpl implements DepartmentDao {
    private static final String SQL_QUERY_SELECT_BY_ID = "SELECT * FROM department WHERE id = ?";
    private static final String SQL_QUERY_SELECT_ALL = "select * from DEPARTMENT";
    private static final String SQL_QUERY_DELETE = "delete from DEPARTMENT where ID = '%s'";
    private static final String SQL_QUERY_UPDATE = "update DEPARTMENT set NAME = '%s', LOCATION = '%s' where ID = '%s'";
    private static final String SQL_QUERY_INSERT = "insert into DEPARTMENT (ID, NAME, LOCATION) values ('%s', '%s', '%s')";

    private static final String COLUMN_ID = "ID";
    private static final String COLUMN_NAME = "NAME";
    private static final String COLUMN_LOCATION = "LOCATION";

    @Override
    public Optional<Department> getById(BigInteger Id) {
        Optional<Department> department = Optional.empty();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_BY_ID)) {
            statement.setLong(1, Id.longValue());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    department = Optional.of(createDepartment(resultSet));
                }
            }
            return department;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getById", e);
        }
    }

    @Override
    public List<Department> getAll() {
        List<Department> departments = new ArrayList<>();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(SQL_QUERY_SELECT_ALL);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                Department department = new Department(
                        new BigInteger(resultSet.getString(COLUMN_ID)),
                        resultSet.getString(COLUMN_NAME),
                        resultSet.getString(COLUMN_LOCATION));
                departments.add(department);
            }
            return departments;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at getAll", e);
        }
    }

    @Override
    public Department save(Department department) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             Statement statement = connection.createStatement()) {
            Optional<Department> foundDepartment = getById(department.getId());
            if (foundDepartment.isPresent()) {
                statement.executeUpdate(String.format(
                        SQL_QUERY_UPDATE,
                        department.getName(),
                        department.getLocation(),
                        department.getId().toString()));
            } else {
                statement.executeUpdate(String.format(
                        SQL_QUERY_INSERT,
                        department.getId().toString(),
                        department.getName(),
                        department.getLocation())
                );
            }
            return department;
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at save", e);
        }
    }

    @Override
    public void delete(Department department) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format(SQL_QUERY_DELETE, department.getId().toString()));
        } catch (SQLException e) {
            throw new DaoException("Something went wrong at delete", e);
        }
    }

    private Department createDepartment(ResultSet resultSet) {
        try {
            BigInteger id = getBigInteger(resultSet, COLUMN_ID);
            String name = resultSet.getString(COLUMN_NAME);
            String location = resultSet.getString(COLUMN_LOCATION);
            return new Department(id, name, location);

        } catch (SQLException e) {
            throw new DaoException("Something went wrong at createDepartment", e);
        }
    }
}