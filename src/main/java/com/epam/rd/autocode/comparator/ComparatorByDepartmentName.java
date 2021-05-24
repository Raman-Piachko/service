package com.epam.rd.autocode.comparator;

import com.epam.rd.autocode.domain.Employee;

import java.util.Comparator;

public class ComparatorByDepartmentName implements Comparator<Employee> {
    @Override
    public int compare(Employee e1, Employee e2) {
        if (e1.getDepartment() != null && e2.getDepartment() != null) {
            return e1.getDepartment().getName().compareTo(e2.getDepartment().getName());
        } else if (e1.getDepartment() == null) {
            return -1;
        } else {
            return 1;
        }
    }
}