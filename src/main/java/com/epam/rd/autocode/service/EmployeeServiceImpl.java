package com.epam.rd.autocode.service;

import com.epam.rd.autocode.comparator.ComparatorByDepartmentName;
import com.epam.rd.autocode.dao.DaoFactory;
import com.epam.rd.autocode.dao.EmployeeDao;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;

import java.util.Comparator;
import java.util.List;

public class EmployeeServiceImpl implements EmployeeService {
    private static final DaoFactory DAO_FACTORY = new DaoFactory();
    private EmployeeDao employeeDao = DAO_FACTORY.employeeDAO();

    @Override
    public List<Employee> getAllSortByHireDate(Paging paging) {
        List<Employee> employees = employeeDao.getAll();
        employees.sort(Comparator.comparing(Employee::getHired));

        return getSublist(employees, paging);
    }

    @Override
    public List<Employee> getAllSortByLastname(Paging paging) {
        List<Employee> employees = employeeDao.getAll();
        employees.sort(Comparator.comparing(e -> e.getFullName().getLastName()));

        return getSublist(employees, paging);
    }

    @Override
    public List<Employee> getAllSortBySalary(Paging paging) {
        List<Employee> employees = employeeDao.getAll();
        employees.sort(Comparator.comparing(Employee::getSalary));

        return getSublist(employees, paging);
    }

    @Override
    public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
        List<Employee> employees = employeeDao.getAll();
        employees.sort(Comparator.comparing(e -> e.getFullName().getLastName()));
        employees.sort(new ComparatorByDepartmentName());

        return getSublist(employees, paging);
    }

    @Override
    public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
        List<Employee> byDepartment = employeeDao.getByDepartment(department);
        byDepartment.sort(Comparator.comparing(Employee::getHired));

        return getSublist(byDepartment, paging);
    }

    @Override
    public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
        List<Employee> byDepartment = employeeDao.getByDepartment(department);
        byDepartment.sort(Comparator.comparing(Employee::getSalary));

        return getSublist(byDepartment, paging);
    }

    @Override
    public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
        List<Employee> byDepartment = employeeDao.getByDepartment(department);
        byDepartment.sort(Comparator.comparing(e -> e.getFullName().getLastName()));

        return getSublist(byDepartment, paging);
    }

    @Override
    public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
        List<Employee> byDepartment = employeeDao.getByManager(manager);
        byDepartment.sort(Comparator.comparing(e -> e.getFullName().getLastName()));

        return getSublist(byDepartment, paging);
    }

    @Override
    public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
        List<Employee> byDepartment = employeeDao.getByManager(manager);
        byDepartment.sort(Comparator.comparing(Employee::getHired));

        return getSublist(byDepartment, paging);
    }

    @Override
    public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
        List<Employee> byDepartment = employeeDao.getByManager(manager);
        byDepartment.sort(Comparator.comparing(Employee::getSalary));

        return getSublist(byDepartment, paging);
    }

    @Override
    public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
        return employeeDao.getByIdWithFullChain(employee.getId());
    }

    @Override
    public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
        List<Employee> employees = employeeDao.getByDepartment(department);
        employees.sort(Comparator.comparing(e -> e.getSalary().negate()));
        int listIndex = salaryRank - 1;
        int lastIndex = employees.size() - 1;
        return employees.get(Math.min(lastIndex, listIndex));
    }

    private <T> List<T> getSublist(List<T> list, Paging paging) {
        int fromIndex = paging.itemPerPage * (paging.page - 1);
        int toIndex = Math.min(list.size(), getToIndex(paging));

        return list.subList(fromIndex, toIndex);
    }

    private int getFromIndex(Paging paging) {
        return paging.itemPerPage * (paging.page - 1);
    }

    private int getToIndex(Paging paging) {
        return paging.itemPerPage + getFromIndex(paging);
    }
}