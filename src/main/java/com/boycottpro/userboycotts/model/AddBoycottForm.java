package com.boycottpro.userboycotts.model;

import java.util.List;

public class AddBoycottForm {
    private String user_id;
    private String company_id;
    private String company_name;
    private List<Reason> reasons;
    private String personal_reason;

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getCompany_id() {
        return company_id;
    }

    public void setCompany_id(String company_id) {
        this.company_id = company_id;
    }

    public String getCompany_name() {
        return company_name;
    }

    public void setCompany_name(String company_name) {
        this.company_name = company_name;
    }

    public List<Reason> getReasons() {
        return reasons;
    }

    public void setReasons(List<Reason> reasons) {
        this.reasons = reasons;
    }

    public String getPersonal_reason() {
        return personal_reason;
    }

    public void setPersonal_reason(String personal_reason) {
        this.personal_reason = personal_reason;
    }

    @Override
    public String toString() {
        return "AddBoycottForm{" +
                "user_id='" + user_id + '\'' +
                ", company_id='" + company_id + '\'' +
                ", company_name='" + company_name + '\'' +
                ", reasons=" + reasons +
                ", personal_reason='" + personal_reason + '\'' +
                '}';
    }

    public static class Reason {
        private String cause_id;
        private String cause_desc;

        public String getCause_id() {
            return cause_id;
        }

        public void setCause_id(String cause_id) {
            this.cause_id = cause_id;
        }

        public String getCause_desc() {
            return cause_desc;
        }

        public void setCause_desc(String cause_desc) {
            this.cause_desc = cause_desc;
        }
    }
}

