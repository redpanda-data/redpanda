import React from "react";
import "./Input.scss";

const Select = (
  props: React.DetailedHTMLProps<
    React.SelectHTMLAttributes<HTMLSelectElement>,
    HTMLSelectElement
  >
) => {
  return <select className="Select" {...props} />;
};

export default Select;
