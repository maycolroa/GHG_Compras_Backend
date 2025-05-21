import { IsEmail, IsString, Matches, MaxLength, MinLength } from "class-validator";

export class CreateUserDto {
    @IsEmail()
    @IsString()
    email: string;
    @IsString()
    @MaxLength(50)
    @Matches(/^(?=.*\d)(?=.*[A-Z])(?=.*[a-z]).*$/, {
        message: 'The password must have an uppercase letter, a lowercase letter, and a number',
      })
    password: string;
    @IsString()
    @MinLength(3)
    fullName: string;
}
