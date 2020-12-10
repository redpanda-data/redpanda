import React from "react";
import "./Input.scss";

const Input = (
  props: React.DetailedHTMLProps<
    React.InputHTMLAttributes<HTMLInputElement>,
    HTMLInputElement
  >
) => {
  return <input className="Input" {...props} />;
};

export default Input;
